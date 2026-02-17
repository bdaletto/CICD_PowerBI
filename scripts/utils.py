import os
import sys
import base64
import json
import zipfile
import tempfile
from typing import List, Dict, Optional
import time
import re
import requests

# Base Fabric REST API
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


class FabricAuthError(Exception):
    """Authentication/Token errors."""
    pass


class FabricApiError(Exception):
    """Fabric REST API call errors."""
    pass

def patch_expressions_tmdl(folder: str, parameters: dict) -> dict:
    """
    Patche les paramètres M dans expressions.tmdl avant déploiement.
    Retourne un dict {chemin: contenu_original} pour restauration.
    """
    expressions_path = os.path.join(folder, "definition", "expressions.tmdl")

    if not os.path.exists(expressions_path):
        print(f"[WARNING] Pas de expressions.tmdl trouve dans {folder}")
        return {}

    with open(expressions_path, 'r', encoding='utf-8') as f:
        original_content = f.read()

    modified_content = original_content

    for param_name, param_value in parameters.items():
        pattern = rf'(expression\s+{param_name}\s*=\s*")[^"]*(")'
        replacement = rf'\g<1>{param_value}\g<2>'

        new_content = re.sub(pattern, replacement, modified_content)

        if new_content != modified_content:
            print(f"   [PATCH] '{param_name}' -> '{param_value}'")
            modified_content = new_content
        else:
            print(f"   [WARNING] Parametre '{param_name}' non trouve dans expressions.tmdl")

    with open(expressions_path, 'w', encoding='utf-8') as f:
        f.write(modified_content)

    print(f"[OK] expressions.tmdl patche avec succes")
    return {expressions_path: original_content}


def restore_tmdl_files(originals: dict) -> None:
    """
    Restaure les fichiers TMDL après déploiement.
    Garantit que le repo reste propre.
    """
    for path, original_content in originals.items():
        with open(path, 'w', encoding='utf-8') as f:
            f.write(original_content)
        print(f"[RESTORE] Restaure: {os.path.basename(path)}")

        
def _get_env_or_fail(name: str) -> str:
    """Get an env var or raise a clear error."""
    value = os.getenv(name)
    if not value:
        raise FabricAuthError(f"Missing environment variable: {name}")
    return value


def get_access_token_spn() -> str:
    """
    Récupère un access token Microsoft Entra pour Fabric en client_credentials
    (Service Principal) vers le scope Fabric: https://api.fabric.microsoft.com/.default
    """
    tenant_id = _get_env_or_fail("FABRIC_TENANT_ID")
    client_id = _get_env_or_fail("FABRIC_CLIENT_ID")
    client_secret = _get_env_or_fail("FABRIC_CLIENT_SECRET")

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://api.fabric.microsoft.com/.default",
    }

    resp = requests.post(token_url, data=data)
    if resp.status_code != 200:
        raise FabricAuthError(
            f"Failed to acquire token. HTTP {resp.status_code}: {resp.text}"
        )

    token = resp.json().get("access_token")
    if not token:
        raise FabricAuthError("Token response does not contain 'access_token'.")
    return token


def fabric_request(method: str, path: str, token: str, **kwargs) -> requests.Response:
    """
    Appelle l'API Fabric REST (Core) :
      - Ajoute automatiquement le header Authorization: Bearer <token>
      - Lève une exception si le status HTTP n'est pas 2xx
    """
    url = f"{FABRIC_API_BASE}/{path.lstrip('/')}"
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"

    if "json" in kwargs and "Content-Type" not in headers:
        headers["Content-Type"] = "application/json"

    print(f"Calling Fabric API: {method} {url}")
    resp = requests.request(method, url, headers=headers, **kwargs)

    if not resp.ok:
        raise FabricApiError(
            f"{method} {url} failed. "
            f"HTTP {resp.status_code}: {resp.text}"
        )

    return resp


def get_or_create_workspace(
    workspace_name: str,
    token: str,
    capacity_id: Optional[str] = None,
) -> str:
    """
    1. Liste les workspaces (GET /workspaces)
    2. Si un workspace avec displayName == workspace_name existe -> retourne son id
    3. Sinon, crée le workspace (POST /workspaces)
    """
    resp = fabric_request("GET", "workspaces", token)
    data = resp.json()

    workspaces = data.get("value", data.get("workspaces", []))

    for ws in workspaces:
        if ws.get("displayName") == workspace_name:
            ws_id = ws.get("id")
            print(f"Workspace '{workspace_name}' already exists (id={ws_id}).")
            return ws_id

    body: Dict[str, object] = {"displayName": workspace_name}
    if capacity_id:
        body["capacityId"] = capacity_id

    print(f"Creating workspace '{workspace_name}'...")
    resp = fabric_request("POST", "workspaces", token, json=body)
    ws = resp.json()
    ws_id = ws["id"]
    print(f"Workspace created (id={ws_id}).")
    return ws_id


def list_items_by_type(
    workspace_id: str,
    item_type: str,
    token: str,
) -> List[Dict]:
    """
    Liste les items d'un workspace filtrés par type (Report, SemanticModel, ...)
      GET /workspaces/{workspaceId}/items?type={item_type}
    """
    path = f"workspaces/{workspace_id}/items?type={item_type}"
    resp = fabric_request("GET", path, token)
    data = resp.json()
    return data.get("value", data.get("items", []))


def build_definition_parts_from_folder(folder: str) -> List[Dict[str, str]]:
    """
    Construit la liste des 'parts' pour un Item Definition à partir d'un dossier PBIP :
      - parcourt tous les fichiers (definition/, StaticResources/, .platform, etc.)
      - crée un part par fichier:
          path       = chemin relatif (style 'definition/report.json')
          payload    = fichier encodé en base64
          payloadType= InlineBase64 (unique valeur supportée)
    """
    parts: List[Dict[str, str]] = []

    for root, _, files in os.walk(folder):
        for filename in files:
            full_path = os.path.join(root, filename)
            rel_path = os.path.relpath(full_path, folder).replace("\\", "/")

            with open(full_path, "rb") as f:
                content = f.read()

            b64 = base64.b64encode(content).decode("ascii")
            parts.append(
                {
                    "path": rel_path,
                    "payload": b64,
                    "payloadType": "InlineBase64",
                }
            )

    if not parts:
        raise ValueError(f"No files found in PBIP folder: {folder}")

    return parts


def wait_for_long_running_operation(
    operation_url: str,
    token: str,
    max_wait_seconds: int = 300,
    poll_interval: int = 5,
) -> Dict:
    """
    Suit une opération longue durée (Long Running Operation - LRO) via son URL.
    L'URL peut pointer vers l'API Fabric OU l'API Power BI (wabi-*).
    """
    print(f"\n⏳ Suivi de l'opération: {operation_url}")
    
    start_time = time.time()
    attempt = 0
    
    while (time.time() - start_time) < max_wait_seconds:
        attempt += 1
        
        try:
            headers = {"Authorization": f"Bearer {token}"}
            resp = requests.get(operation_url, headers=headers)
            
            print(f"   [{attempt}] GET {operation_url} -> HTTP {resp.status_code}")
            
            if not resp.ok:
                print(f"   ⚠️ Erreur HTTP {resp.status_code}: {resp.text}")
                time.sleep(poll_interval)
                continue
            
            operation_status = resp.json()
        except Exception as e:
            print(f"   ⚠️ Erreur lors du polling (tentative {attempt}): {e}")
            time.sleep(poll_interval)
            continue
        
        status = operation_status.get("status", "").lower()
        percent = operation_status.get("percentComplete", 0)
        
        if attempt == 1 or attempt % 10 == 0:
            print(f"   Réponse opération: {json.dumps(operation_status, indent=2)}")
        
        print(f"   [{attempt}] Status: {status} ({percent}%)")
        
        if status in ["succeeded", "completed"]:
            print("   ✅ Opération terminée avec succès")
            return operation_status
        
        elif status in ["failed", "cancelled"]:
            error_info = operation_status.get("error", operation_status)
            print(f"\n❌ ÉCHEC DE L'OPÉRATION:")
            print(f"   Status: {status}")
            print(f"   Error: {json.dumps(error_info, indent=2)}")
            raise FabricApiError(
                f"Opération {status}: {json.dumps(error_info)}"
            )
        
        elif status in ["running", "notstarted", "inprogress"]:
            time.sleep(poll_interval)
            continue
        
        else:
            print(f"   ⚠️ Statut inconnu: {status}, on continue...")
            time.sleep(poll_interval)
    
    raise FabricApiError(f"⏱️ Timeout après {max_wait_seconds}s")


def find_dataset_cross_workspace(
    dataset_name: str,
    workspace_id: str,
    token: str
) -> Optional[str]:
    """
    Cherche un dataset par nom dans un workspace spécifique.
    Retourne le dataset_id ou None si introuvable.
    """
    print(f"🔍 Recherche du dataset '{dataset_name}' dans workspace {workspace_id}...")
    
    try:
        items = list_items_by_type(workspace_id, "SemanticModel", token)
        for item in items:
            if item.get("displayName") == dataset_name:
                dataset_id = item["id"]
                print(f"✅ Dataset trouvé: {dataset_name} (id={dataset_id})")
                return dataset_id
        
        print(f"❌ Dataset '{dataset_name}' introuvable dans ce workspace")
        return None
    except Exception as e:
        print(f"⚠️ Erreur lors de la recherche: {e}")
        return None


def rebind_report_cross_workspace(
    report_workspace_id: str,
    report_id: str,
    dataset_workspace_id: str,
    dataset_id: str,
    token: str
) -> None:
    """
    Relie un rapport à un dataset qui est dans un AUTRE workspace.
    Utilise l'API Power BI Rebind qui supporte le cross-workspace.
    
    Doc: https://learn.microsoft.com/en-us/rest/api/power-bi/reports/rebind-report-in-group
    """
    print(f"\n🔗 Liaison cross-workspace du rapport au dataset...")
    print(f"   Report workspace: {report_workspace_id}")
    print(f"   Report ID: {report_id}")
    print(f"   Dataset workspace: {dataset_workspace_id}")
    print(f"   Dataset ID: {dataset_id}")
    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{report_workspace_id}/reports/{report_id}/Rebind"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    body = {
        "datasetId": dataset_id
    }
    
    # Si le dataset est dans un autre workspace
    if dataset_workspace_id != report_workspace_id:
        print(f"   ⚠️ Cross-workspace binding détecté")
    
    resp = requests.post(url, headers=headers, json=body)
    
    if resp.ok:
        print(f"✅ Rapport lié au dataset (cross-workspace)")
    else:
        print(f"❌ ÉCHEC du rebind: HTTP {resp.status_code}")
        print(f"   Response: {resp.text}")
        
        # Vérifier les permissions
        if resp.status_code == 403:
            print(f"   💡 Le Service Principal a-t-il accès aux DEUX workspaces ?")
        
        raise FabricApiError(f"Failed to rebind report: {resp.status_code}")


# ============================================================
# DÉPLOIEMENT DE RAPPORTS VIA API POWER BI
# ============================================================

def create_pbix_from_pbip(pbip_folder: str, output_path: Optional[str] = None) -> str:
    """
    Crée un fichier .pbix temporaire depuis un dossier .Report PBIP.
    Le .pbix est un ZIP contenant tous les fichiers du PBIP.
    """
    if output_path is None:
        output_path = tempfile.mktemp(suffix=".pbix")
    
    print(f"📦 Création du .pbix depuis {pbip_folder}...")
    
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(pbip_folder):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, pbip_folder)
                zipf.write(file_path, arcname)
    
    file_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"✅ .pbix créé: {output_path} ({file_size:.2f} MB)")
    return output_path


def upload_pbix_via_powerbi_api(
    workspace_id: str,
    pbix_path: str,
    report_name: str,
    token: str,
) -> str:
    """
    Upload un fichier .pbix via l'API Power BI Import.
    
    Doc: https://learn.microsoft.com/en-us/rest/api/power-bi/imports/post-import-in-group
    """
    print(f"\n📤 Upload du rapport via Power BI API...")
    print(f"   Fichier: {pbix_path}")
    print(f"   Nom: {report_name}")
    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/imports"
    
    params = {
        "datasetDisplayName": report_name,
        "nameConflict": "CreateOrOverwrite",
    }
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    with open(pbix_path, 'rb') as f:
        files = {
            'file': (os.path.basename(pbix_path), f, 'application/octet-stream')
        }
        
        resp = requests.post(url, headers=headers, params=params, files=files)
    
    if not resp.ok:
        raise FabricApiError(
            f"Failed to upload .pbix: HTTP {resp.status_code}\n{resp.text}"
        )
    
    import_info = resp.json()
    import_id = import_info.get("id")
    
    print(f"✅ Import démarré (id={import_id})")
    
    print(f"⏳ Attente de la fin de l'import...")
    return wait_for_import_completion(workspace_id, import_id, token)


def wait_for_import_completion(
    workspace_id: str,
    import_id: str,
    token: str,
    max_wait: int = 300
) -> str:
    """
    Attend la fin d'un import Power BI et retourne le report_id.
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/imports/{import_id}"
    headers = {"Authorization": f"Bearer {token}"}
    
    start = time.time()
    while (time.time() - start) < max_wait:
        resp = requests.get(url, headers=headers)
        if not resp.ok:
            print(f"⚠️ Erreur vérification import: {resp.status_code}")
            time.sleep(5)
            continue
        
        import_status = resp.json()
        status = import_status.get("importState", "Unknown")
        
        print(f"   Status: {status}")
        
        if status == "Succeeded":
            reports = import_status.get("reports", [])
            if reports:
                report_id = reports[0]["id"]
                report_name = reports[0].get("name", "")
                print(f"✅ Import terminé - Report: {report_name} (id={report_id})")
                return report_id
            else:
                raise FabricApiError("Import succeeded but no report found")
        
        elif status == "Failed":
            error = import_status.get("error", "Unknown error")
            raise FabricApiError(f"Import failed: {error}")
        
        time.sleep(5)
    
    raise FabricApiError(f"Import timeout after {max_wait}s")


def rebind_report_to_dataset(
    workspace_id: str,
    report_id: str,
    dataset_id: str,
    token: str
) -> None:
    """
    Relie un rapport à un dataset via l'API Power BI.
    Doc: https://learn.microsoft.com/en-us/rest/api/power-bi/reports/rebind-report-in-group
    """
    print(f"\n🔗 Liaison du rapport au dataset...")
    print(f"   Report ID: {report_id}")
    print(f"   Dataset ID: {dataset_id}")
    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/Rebind"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {
        "datasetId": dataset_id
    }
    
    print(f"   POST {url}")
    resp = requests.post(url, headers=headers, json=body)
    
    print(f"   Response: HTTP {resp.status_code}")
    
    if resp.ok:
        print(f"✅ Rapport lié au dataset avec succès")
    else:
        print(f"❌ ÉCHEC du rebind: HTTP {resp.status_code}")
        print(f"   Response: {resp.text}")
        
        # Essayer de vérifier le lien actuel
        try:
            check_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}"
            check_resp = requests.get(check_url, headers={"Authorization": f"Bearer {token}"})
            if check_resp.ok:
                report_info = check_resp.json()
                current_dataset = report_info.get("datasetId", "unknown")
                print(f"   Dataset actuel du rapport: {current_dataset}")
                print(f"   Dataset cible: {dataset_id}")
        except Exception as e:
            print(f"   Impossible de vérifier: {e}")
        
        raise FabricApiError(f"Failed to rebind report: {resp.status_code} - {resp.text}")


def deploy_report_via_fabric_workaround(
    workspace_id: str,
    pbip_folder: str,
    token: str,
    dataset_id: Optional[str] = None,
) -> str:
    """
    Déploie un rapport en contournant le problème de definition.pbir.
    Stratégie: Modifier la référence au dataset avec le GUID, créer le rapport,
    puis le relier au dataset via rebind.
    
    Si dataset_id n'est pas fourni, lève une erreur car le rapport nécessite un dataset.
    """
    report_name = os.path.basename(pbip_folder).replace(".Report", "")
    
    if not dataset_id:
        print(f"⚠️ Aucun dataset_id fourni pour le rapport '{report_name}'")
        print(f"   Le rapport sera créé mais pourrait ne pas fonctionner sans dataset")
    
    # Construire les parts en modifiant temporairement definition.pbir
    print(f"🔧 Modification de definition.pbir...")
    parts = []
    
    for root, _, files in os.walk(pbip_folder):
        for filename in files:
            full_path = os.path.join(root, filename)
            rel_path = os.path.relpath(full_path, pbip_folder).replace("\\", "/")
            
            with open(full_path, "rb") as f:
                content = f.read()
            
            # Si c'est definition.pbir, on le modifie pour utiliser byConnection avec le dataset GUID
            if rel_path == "definition.pbir" and dataset_id:
                pbir_modified = json.loads(content.decode('utf-8'))
                # Utiliser le GUID du dataset directement dans connectionString
                pbir_modified["datasetReference"] = {
                    "byConnection": {
                        "connectionString": f"semanticModelId={dataset_id}"
                    }
                }
                content = json.dumps(pbir_modified, indent=2).encode('utf-8')
                print(f"   ✏️ definition.pbir modifié avec dataset GUID: {dataset_id}")
            
            b64 = base64.b64encode(content).decode("ascii")
            parts.append({
                "path": rel_path,
                "payload": b64,
                "payloadType": "InlineBase64",
            })
    
    definition = {"parts": parts}
    
    # Vérifier si le rapport existe déjà
    print(f"\n🔍 Vérification si '{report_name}' existe déjà...")
    existing_items = list_items_by_type(workspace_id, "Report", token)
    item_id = None
    for it in existing_items:
        if it.get("displayName") == report_name:
            item_id = it["id"]
            break
    
    # Créer ou mettre à jour
    if item_id is None:
        print(f"➕ Création du rapport...")
        body = {
            "displayName": report_name,
            "type": "Report",
            "definition": definition,
        }
        
        resp = fabric_request("POST", f"workspaces/{workspace_id}/items", token, json=body)
        
        if resp.status_code == 202:
            location = resp.headers.get("Location")
            if location:
                retry_after = int(resp.headers.get("Retry-After", "5"))
                time.sleep(retry_after)
                wait_for_long_running_operation(location, token)
                
                # Trouver le rapport créé
                time.sleep(3)
                items = list_items_by_type(workspace_id, "Report", token)
                for it in items:
                    if it.get("displayName") == report_name:
                        item_id = it["id"]
                        break
        elif resp.status_code == 201:
            item = resp.json()
            item_id = item["id"]
    else:
        print(f"🔄 Mise à jour du rapport existant...")
        body = {"definition": definition}
        resp = fabric_request(
            "POST",
            f"workspaces/{workspace_id}/items/{item_id}/updateDefinition?updateMetadata=false",
            token,
            json=body
        )
        if resp.status_code == 202:
            location = resp.headers.get("Location")
            if location:
                wait_for_long_running_operation(location, token)
    
    if not item_id:
        raise FabricApiError("Failed to create or find report")
    
    print(f"✅ Rapport créé/mis à jour: {report_name} (id={item_id})")
    
    # Maintenant rebinder au bon dataset
    print(f"\n🔗 Liaison au dataset...")
    try:
        rebind_report_to_dataset(workspace_id, item_id, dataset_id, token)
    except Exception as e:
        print(f"⚠️ Échec du rebind: {e}")
        print(f"   Le rapport a été créé/mis à jour mais le rebind a échoué")
    
    return item_id


# ============================================================
# FONCTION PRINCIPALE DE DÉPLOIEMENT
# ============================================================

def create_or_update_item_from_folder(
    workspace_id: str,
    folder: str,
    item_type: str,
    token: str,
) -> str:
    display_name = os.path.basename(folder)
    if "." in display_name:
        display_name = display_name.split(".", 1)[0]

    print(f"\n{'='*60}")
    print(f"📦 Publishing {item_type}: {display_name}")
    print(f"   Folder: {folder}")
    print(f"{'='*60}")

    # === TRAITEMENT SPÉCIAL POUR LES REPORTS ===
    if item_type == "Report":
        print("🎯 Déploiement rapport avec workaround Fabric")
        return deploy_report_via_fabric_workaround(workspace_id, folder, token)

    # === TRAITEMENT NORMAL POUR LES AUTRES ITEMS (SemanticModel, etc.) ===
    parts = build_definition_parts_from_folder(folder)
    print(f"   📄 {len(parts)} fichiers encodés")
    
    for part in parts[:5]:
        print(f"      - {part['path']}")
    if len(parts) > 5:
        print(f"      ... et {len(parts) - 5} autres fichiers")
    
    definition = {"parts": parts}

    print(f"\n🔍 Vérification si '{display_name}' existe déjà...")
    existing_items = list_items_by_type(workspace_id, item_type, token)
    item_id = None
    for it in existing_items:
        if it.get("displayName") == display_name:
            item_id = it["id"]
            break

    # -------------------------
    # CASE 1 : CREATE
    # -------------------------
    if item_id is None:
        print(f"➕ Item n'existe pas, création en cours...")
        
        body = {
            "displayName": display_name,
            "type": item_type,
            "definition": definition,
        }

        resp = fabric_request(
            "POST",
            f"workspaces/{workspace_id}/items",
            token,
            json=body,
        )

        status_code = resp.status_code
        print(f"\n📡 Réponse Fabric: HTTP {status_code}")

        if status_code == 201:
            try:
                item = resp.json()
                item_id = item["id"]
                print(f"✅ Créé immédiatement (201) - id={item_id}")
                return item_id
            except Exception as e:
                print(f"❌ Erreur parsing JSON: {e}")
                raise FabricApiError("Failed to parse 201 response")
        
        elif status_code == 202:
            print("⏳ Création asynchrone (202 Accepted)")
            
            location = resp.headers.get("Location")
            retry_after = resp.headers.get("Retry-After", "5")
            
            if location:
                print(f"   Location header trouvé: {location}")
                print(f"   Retry-After: {retry_after}s")
                
                try:
                    time.sleep(int(retry_after))
                    wait_for_long_running_operation(location, token)
                    
                    print("\n🔍 Recherche de l'item créé...")
                    time.sleep(3)
                    
                    items = list_items_by_type(workspace_id, item_type, token)
                    for it in items:
                        if it.get("displayName") == display_name:
                            item_id = it["id"]
                            print(f"✅ Item trouvé après opération async - id={item_id}")
                            return item_id
                    
                    raise FabricApiError(
                        f"Opération réussie mais item '{display_name}' introuvable"
                    )
                    
                except Exception as e:
                    print(f"\n❌ Erreur lors du suivi de l'opération: {e}")
                    raise
            
            else:
                print("⚠️ PAS DE LOCATION HEADER!")
                raise FabricApiError("202 response without Location header")
        
        else:
            print(f"❌ Code HTTP inattendu: {status_code}")
            print(resp.text)
            raise FabricApiError(f"Unexpected status code {status_code}")

    # -------------------------
    # CASE 2 : UPDATE
    # -------------------------
    print(f"🔄 Item existe déjà (id={item_id}), mise à jour...")
    
    body = {"definition": definition}

    resp = fabric_request(
        "POST",
        f"workspaces/{workspace_id}/items/{item_id}/updateDefinition?updateMetadata=false",
        token,
        json=body,
    )

    status_code = resp.status_code
    print(f"📡 Réponse mise à jour: HTTP {status_code}")

    if status_code == 200:
        print(f"✅ Mis à jour immédiatement (200)")
        return item_id
    
    elif status_code == 202:
        location = resp.headers.get("Location")
        if location:
            wait_for_long_running_operation(location, token)
        else:
            print("⚠️ Pas de Location, attente arbitraire de 10s...")
            time.sleep(10)
        
        print(f"✅ Mis à jour (async)")
        return item_id
    
    else:
        print(f"❌ Mise à jour échouée: {status_code}")
        print(resp.text)
        raise FabricApiError(f"Update failed with status {status_code}")
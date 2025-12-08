import os
import sys
import base64
import json
from typing import List, Dict, Optional
import time

import requests

# Base Fabric REST API
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


class FabricAuthError(Exception):
    """Authentication/Token errors."""
    pass


class FabricApiError(Exception):
    """Fabric REST API call errors."""
    pass


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

    # Si on envoie un body, on s'assure du content-type JSON
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
    # 1. List workspaces
    resp = fabric_request("GET", "workspaces", token)
    data = resp.json()

    workspaces = data.get("value", data.get("workspaces", []))

    for ws in workspaces:
        if ws.get("displayName") == workspace_name:
            ws_id = ws.get("id")
            print(f"Workspace '{workspace_name}' already exists (id={ws_id}).")
            return ws_id

    # 2. Create workspace
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

def get_workspace_name_from_id(workspace_id: str, token: str) -> str:
    """
    Récupère le nom d'un workspace à partir de son ID.
    """
    resp = fabric_request("GET", f"workspaces/{workspace_id}", token)
    workspace = resp.json()
    return workspace.get("displayName", workspace_id)

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
    
    Doc: https://learn.microsoft.com/en-us/rest/api/fabric/articles/long-running-operation
    """
    print(f"\n⏳ Suivi de l'opération: {operation_url}")
    
    start_time = time.time()
    attempt = 0
    
    while (time.time() - start_time) < max_wait_seconds:
        attempt += 1
        
        try:
            # Appeler directement l'URL complète (pas via fabric_request qui ajoute le base URL)
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
        
        # Pour debug: afficher la réponse complète
        if attempt == 1 or attempt % 10 == 0:
            print(f"   Réponse opération: {json.dumps(operation_status, indent=2)}")
        
        print(f"   [{attempt}] Status: {status} ({percent}%)")
        
        # Status possibles: NotStarted, Running, Succeeded, Failed, Undefined
        # Power BI peut aussi retourner: InProgress, Completed
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

def fix_definition_pbir(
    parts: List[Dict[str, str]], 
    workspace_id: str,
    token: str
) -> List[Dict[str, str]]:
    """
    Remplace byPath par byConnection avec le format correct dans definition.pbir.
    Utilise workspace_id pour construire la connectionString.
    """
    # Récupérer le nom du workspace
    workspace_name = get_workspace_name_from_id(workspace_id, token)
    print(f"🔧 Workspace name: {workspace_name}")
    
    fixed_parts = []
    
    for part in parts:
        if part["path"] == "definition.pbir":
            # Décoder le contenu
            content = base64.b64decode(part["payload"]).decode("utf-8")
            pbir = json.loads(content)
            
            # Extraire le nom du dataset depuis l'ancien byPath (si présent)
            dataset_name = None
            if "datasetReference" in pbir:
                if "byPath" in pbir["datasetReference"]:
                    old_path = pbir["datasetReference"]["byPath"].get("path", "")
                    if old_path:
                        # Ex: "../Mon_Dataset.SemanticModel" -> "Mon_Dataset"
                        dataset_name = old_path.split("/")[-1].replace(".SemanticModel", "")
            
            # Si pas de dataset trouvé, essayer de deviner depuis le dossier
            if not dataset_name:
                # Fallback: chercher un .SemanticModel dans le workspace
                print("⚠️ Impossible d'extraire le nom du dataset depuis byPath")
                dataset_name = "DATASET_NAME_PLACEHOLDER"
            
            print(f"🔧 Conversion byPath -> byConnection")
            print(f"   Dataset: {dataset_name}")
            print(f"   Workspace: {workspace_name}")
            
            # Remplacer par le format correct
            pbir["datasetReference"] = {
                "byConnection": {
                    "connectionString": f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace_name};Initial Catalog={dataset_name}"
                }
            }
            
            # Ré-encoder
            new_content = json.dumps(pbir, indent=2)
            new_b64 = base64.b64encode(new_content.encode("utf-8")).decode("ascii")
            
            fixed_parts.append({
                "path": part["path"],
                "payload": new_b64,
                "payloadType": "InlineBase64"
            })
        else:
            fixed_parts.append(part)
    
    return fixed_parts

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

    parts = build_definition_parts_from_folder(folder)
    if item_type == "Report":
        parts = fix_definition_pbir(parts, workspace_id, token)
        print("Modified definition.pbir to include reference to semantic model")
    print(f"   📄 {len(parts)} fichiers encodés")
    
    # Afficher les fichiers pour debug
    for part in parts[:5]:  # Limiter à 5 pour pas polluer
        print(f"      - {part['path']}")
    if len(parts) > 5:
        print(f"      ... et {len(parts) - 5} autres fichiers")
    
    definition = {"parts": parts}

    # Check if exists
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
        
        # AFFICHER TOUS LES HEADERS POUR DEBUG
        print(f"📋 Headers de réponse:")
        for header, value in resp.headers.items():
            print(f"   {header}: {value}")

        # Cas 1: Création synchrone réussie (201)
        if status_code == 201:
            try:
                item = resp.json()
                item_id = item["id"]
                print(f"✅ Créé immédiatement (201) - id={item_id}")
                return item_id
            except Exception as e:
                print(f"❌ Erreur parsing JSON: {e}")
                print(f"Raw response: {resp.text}")
                raise FabricApiError("Failed to parse 201 response")
        
        # Cas 2: Création asynchrone (202)
        elif status_code == 202:
            print("⏳ Création asynchrone (202 Accepted)")
            
            # Chercher l'URL de l'opération
            location = resp.headers.get("Location")
            retry_after = resp.headers.get("Retry-After", "5")
            
            if location:
                print(f"   Location header trouvé: {location}")
                print(f"   Retry-After: {retry_after}s")
                
                try:
                    # Attendre un peu avant de commencer le polling
                    time.sleep(int(retry_after))
                    
                    # Suivre l'opération
                    operation_result = wait_for_long_running_operation(location, token)
                    
                    # Récupérer l'item créé
                    print("\n🔍 Recherche de l'item créé...")
                    time.sleep(3)  # Attendre que l'item soit bien visible
                    
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
                    print("📋 Contenu de la réponse 202:")
                    print(resp.text)
                    raise
            
            else:
                print("⚠️ PAS DE LOCATION HEADER!")
                print("📋 Contenu de la réponse 202:")
                print(resp.text)
                
                # Fallback: polling manuel
                print("\n⚠️ Fallback: polling manuel des items...")
                return _wait_for_item_manual_polling(
                    workspace_id, display_name, item_type, token
                )
        
        # Cas 3: Code inattendu
        else:
            print(f"❌ Code HTTP inattendu: {status_code}")
            print(f"📋 Réponse complète:")
            print(resp.text)
            raise FabricApiError(
                f"Unexpected status code {status_code} for item creation"
            )

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


def _wait_for_item_manual_polling(
    workspace_id: str,
    display_name: str,
    item_type: str,
    token: str,
    max_attempts: int = 60,
) -> str:
    """
    Fallback: polling manuel si pas de Location header.
    """
    print(f"⏳ Attente manuelle de la création (max {max_attempts * 5}s)...")
    
    for attempt in range(1, max_attempts + 1):
        time.sleep(5)
        
        try:
            items = list_items_by_type(workspace_id, item_type, token)
            for it in items:
                if it.get("displayName") == display_name:
                    item_id = it["id"]
                    print(f"✅ Item trouvé après {attempt * 5}s (id={item_id})")
                    return item_id
        except Exception as e:
            print(f"   ⚠️ Erreur lors du polling (tentative {attempt}): {e}")
        
        if attempt % 6 == 0:  # Log toutes les 30s
            print(f"   Toujours en attente... ({attempt * 5}s écoulées)")
    
    # Timeout - afficher les items existants pour debug
    print(f"\n❌ TIMEOUT après {max_attempts * 5}s")
    print(f"🔍 Items {item_type} actuels dans le workspace:")
    try:
        items = list_items_by_type(workspace_id, item_type, token)
        if not items:
            print("   (aucun item)")
        for it in items:
            print(f"   - {it.get('displayName')} (id={it.get('id')})")
    except Exception as e:
        print(f"   Erreur lors de la récupération des items: {e}")
    
    raise FabricApiError(
        f"Timeout: {item_type} '{display_name}' non créé. "
        "Vérifier les logs Fabric et les permissions du Service Principal."
    )
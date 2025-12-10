import argparse
import os
import yaml
import glob
from pathlib import Path
from typing import Dict, Optional

from utils import (
    get_access_token_spn,
    create_or_update_item_from_folder,
    list_items_by_type,
    deploy_report_via_fabric_workaround,
    find_dataset_cross_workspace,
    rebind_report_cross_workspace,
)


def load_workspace_mapping(mapping_file: str = "workspace-mapping.yml") -> dict:
    """
    Charge le fichier de mapping des artefacts vers les workspaces.
    """
    if not os.path.exists(mapping_file):
        raise FileNotFoundError(f"Fichier de mapping introuvable: {mapping_file}")
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_workspace_for_artifact(
    artifact_name: str,
    item_type: str,
    environment: str,
    mapping: dict
) -> str:
    """
    Retourne le workspace_id pour un artefact donné dans un environnement.
    Gère la séparation SemanticModel / Report.
    """
    # Déterminer la clé du type d'item
    type_key = "semanticmodel" if item_type == "SemanticModel" else "report"
    
    # Chercher le mapping spécifique
    if artifact_name in mapping:
        artifact_config = mapping[artifact_name]
        
        # Nouveau format avec séparation semanticmodel/report
        if type_key in artifact_config:
            workspace_id = artifact_config[type_key].get(environment)
            if workspace_id:
                print(f"✅ Workspace trouvé pour '{artifact_name}' [{item_type}] ({environment}): {workspace_id}")
                return workspace_id
        
        # Ancien format (rétrocompatibilité) : directement dev/prp/prd
        elif environment in artifact_config:
            workspace_id = artifact_config.get(environment)
            if workspace_id:
                print(f"✅ Workspace trouvé (format legacy) pour '{artifact_name}' ({environment}): {workspace_id}")
                return workspace_id
    
    # Fallback sur le workspace par défaut
    if "default" in mapping:
        default_config = mapping["default"]
        
        # Nouveau format avec séparation
        if type_key in default_config:
            workspace_id = default_config[type_key].get(environment)
            if workspace_id:
                print(f"⚠️ Utilisation du workspace par défaut [{item_type}] pour '{artifact_name}': {workspace_id}")
                return workspace_id
        
        # Ancien format
        elif environment in default_config:
            workspace_id = default_config.get(environment)
            if workspace_id:
                print(f"⚠️ Utilisation du workspace par défaut (legacy) pour '{artifact_name}': {workspace_id}")
                return workspace_id
    
    raise ValueError(
        f"Aucun workspace trouvé pour '{artifact_name}' [{item_type}] dans l'environnement '{environment}'"
    )


def get_dataset_location_for_artifact(
    artifact_name: str,
    environment: str,
    mapping: dict
) -> Optional[dict]:
    """
    Récupère les infos de localisation du dataset pour un artefact.
    Retourne un dict avec workspace_id et dataset_name, ou None.
    """
    if artifact_name not in mapping:
        return None
    
    artifact_config = mapping[artifact_name]
    
    # Vérifier si dataset_location est défini
    if "dataset_location" in artifact_config:
        location = artifact_config["dataset_location"].get(environment)
        if location:
            print(f"📍 Dataset location trouvée pour '{artifact_name}' ({environment}):")
            print(f"   Workspace: {location.get('workspace_id')}")
            print(f"   Dataset: {location.get('dataset_name')}")
            return location
    
    # Fallback 1: même workspace que le SemanticModel (nouveau format)
    if "semanticmodel" in artifact_config:
        sm_workspace = artifact_config["semanticmodel"].get(environment)
        if sm_workspace:
            print(f"📍 Fallback: Dataset dans le même workspace que le SemanticModel")
            return {
                "workspace_id": sm_workspace,
                "dataset_name": artifact_name  # Par défaut, même nom
            }
    
    # Fallback 2: format legacy (directement dev/prp/prd)
    if environment in artifact_config:
        workspace_id = artifact_config.get(environment)
        if workspace_id:
            print(f"📍 Fallback (legacy): Dataset dans le même workspace")
            return {
                "workspace_id": workspace_id,
                "dataset_name": artifact_name  # Par défaut, même nom
            }
    
    return None


def deploy_report_with_cross_workspace_dataset(
    report_workspace_id: str,
    folder: str,
    artifact_name: str,
    environment: str,
    workspace_mapping: dict,
    token: str
) -> str:
    """
    Déploie un rapport et le lie à un dataset qui peut être dans un autre workspace.
    """
    report_name = os.path.basename(folder).replace(".Report", "")
    
    # 1. Trouver où est le dataset
    dataset_location = get_dataset_location_for_artifact(
        artifact_name, environment, workspace_mapping
    )
    
    if not dataset_location:
        print(f"⚠️ Aucune localisation de dataset trouvée pour '{artifact_name}'")
        print(f"   Le rapport sera déployé sans lien au dataset")
        dataset_workspace_id = None
        dataset_id = None
    else:
        dataset_workspace_id = dataset_location["workspace_id"]
        dataset_name = dataset_location["dataset_name"]
        
        # 2. Chercher le dataset dans son workspace
        dataset_id = find_dataset_cross_workspace(
            dataset_name=dataset_name,
            workspace_id=dataset_workspace_id,
            token=token
        )
        
        if not dataset_id:
            print(f"❌ Dataset '{dataset_name}' introuvable dans workspace {dataset_workspace_id}")
            print(f"   Assure-toi que le SemanticModel est déployé AVANT le rapport")
            raise ValueError(f"Dataset not found for report '{artifact_name}'")
    
    # 3. Déployer le rapport (avec workaround definition.pbir)
    print(f"\n📦 Déploiement du rapport...")
    report_id = deploy_report_via_fabric_workaround(
        workspace_id=report_workspace_id,
        pbip_folder=folder,
        token=token,
        dataset_id=dataset_id  # Passer le dataset_id pour modification du definition.pbir
    )
    
    # 4. Rebind cross-workspace si nécessaire
    if dataset_id:
        print(f"\n🔗 Liaison du rapport au dataset...")
        
        if dataset_workspace_id == report_workspace_id:
            print(f"   ℹ️ Même workspace - rebind standard")
        else:
            print(f"   ⚠️ Cross-workspace - vérifier les permissions")
        
        try:
            rebind_report_cross_workspace(
                report_workspace_id=report_workspace_id,
                report_id=report_id,
                dataset_workspace_id=dataset_workspace_id,
                dataset_id=dataset_id,
                token=token
            )
        except Exception as e:
            print(f"⚠️ Échec du rebind: {e}")
            print(f"   Le rapport existe mais le rebind a échoué")
            print(f"   Tu devras peut-être le faire manuellement dans Fabric")
    
    return report_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deploy Power BI artifacts to Fabric with environment mapping"
    )
    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "prp", "prd"],
        help="Environnement de déploiement",
    )
    parser.add_argument(
        "--workspace-mapping",
        default="workspace-mapping.yml",
        help="Fichier de mapping des workspaces",
    )

    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"🚀 DÉPLOIEMENT VERS {args.env.upper()}")
    print(f"{'='*60}\n")

    # 1. Authentification
    print("🔐 Authentification Service Principal...")
    token = get_access_token_spn()
    print("✅ Authentification réussie\n")

    # 2. Charger le mapping
    print(f"📋 Chargement du mapping: {args.workspace_mapping}")
    workspace_mapping = load_workspace_mapping(args.workspace_mapping)
    print(f"✅ {len([k for k in workspace_mapping.keys() if k != 'default'])} artefacts mappés\n")

    # 3. Déployer les SemanticModels
    semantic_folders = glob.glob(os.path.join("src", "*.SemanticModel"))
    
    if semantic_folders:
        print(f"\n📊 Déploiement de {len(semantic_folders)} SemanticModel(s)...")
        for folder in semantic_folders:
            artifact_name = Path(folder).stem  # Nom sans extension
            
            try:
                workspace_id = get_workspace_for_artifact(
                    artifact_name=artifact_name,
                    item_type="SemanticModel",
                    environment=args.env,
                    mapping=workspace_mapping
                )
                
                create_or_update_item_from_folder(
                    workspace_id=workspace_id,
                    folder=folder,
                    item_type="SemanticModel",
                    token=token,
                )
            except Exception as e:
                print(f"❌ Échec pour {artifact_name}: {e}")
                # Continuer avec les autres artefacts
                continue
    else:
        print("\nℹ️ Aucun SemanticModel à déployer")
    
    # 4. Déployer les Reports avec gestion cross-workspace
    report_folders = glob.glob(os.path.join("src", "*.Report"))
    
    if report_folders:
        print(f"\n📈 Déploiement de {len(report_folders)} Report(s)...")
        for folder in report_folders:
            artifact_name = Path(folder).stem  # Nom sans extension
            
            try:
                # Récupérer le workspace du rapport
                report_workspace_id = get_workspace_for_artifact(
                    artifact_name=artifact_name,
                    item_type="Report",
                    environment=args.env,
                    mapping=workspace_mapping
                )
                
                # Déployer avec gestion cross-workspace
                deploy_report_with_cross_workspace_dataset(
                    report_workspace_id=report_workspace_id,
                    folder=folder,
                    artifact_name=artifact_name,
                    environment=args.env,
                    workspace_mapping=workspace_mapping,
                    token=token
                )
            except Exception as e:
                print(f"❌ Échec pour {artifact_name}: {e}")
                continue
    else:
        print("\nℹ️ Aucun Report à déployer")
    
    print(f"\n{'='*60}")
    print(f"✅ DÉPLOIEMENT {args.env.upper()} TERMINÉ")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
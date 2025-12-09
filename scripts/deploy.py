import argparse
import os
import yaml
import glob
from pathlib import Path

from utils import (
    get_access_token_spn,
    create_or_update_item_from_folder,
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
    environment: str,
    mapping: dict
) -> str:
    """
    Retourne le workspace_id pour un artefact donné dans un environnement.
    Utilise le workspace par défaut si l'artefact n'est pas mappé.
    """
    if artifact_name in mapping:
        workspace_id = mapping[artifact_name].get(environment)
        if workspace_id:
            print(f"✅ Workspace trouvé pour '{artifact_name}' ({environment}): {workspace_id}")
            return workspace_id
    
    # Fallback sur le workspace par défaut
    if "default" in mapping:
        workspace_id = mapping["default"].get(environment)
        if workspace_id:
            print(f"⚠️ Utilisation du workspace par défaut pour '{artifact_name}': {workspace_id}")
            return workspace_id
    
    raise ValueError(
        f"Aucun workspace trouvé pour '{artifact_name}' dans l'environnement '{environment}'"
    )


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
        "--mapping-file",
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
    print(f"📋 Chargement du mapping: {args.mapping_file}")
    mapping = load_workspace_mapping(args.mapping_file)
    print(f"✅ {len(mapping) - 1} artefacts mappés\n")  # -1 pour exclure 'default'

    # 3. Déployer les SemanticModels
    semantic_folders = glob.glob(os.path.join("src", "*.SemanticModel"))
    
    if semantic_folders:
        print(f"\n📊 Déploiement de {len(semantic_folders)} SemanticModel(s)...")
        for folder in semantic_folders:
            artifact_name = Path(folder).stem  # Nom sans extension
            
            try:
                workspace_id = get_workspace_for_artifact(
                    artifact_name, args.env, mapping
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
    
    # 4. Déployer les Reports
    report_folders = glob.glob(os.path.join("src", "*.Report"))
    
    if report_folders:
        print(f"\n📈 Déploiement de {len(report_folders)} Report(s)...")
        for folder in report_folders:
            artifact_name = Path(folder).stem  # Nom sans extension
            
            try:
                workspace_id = get_workspace_for_artifact(
                    artifact_name, args.env, mapping
                )
                
                create_or_update_item_from_folder(
                    workspace_id=workspace_id,
                    folder=folder,
                    item_type="Report",
                    token=token,
                )
            except Exception as e:
                print(f"❌ Échec pour {artifact_name}: {e}")
                continue
    
    print(f"\n{'='*60}")
    print(f"✅ DÉPLOIEMENT {args.env.upper()} TERMINÉ")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
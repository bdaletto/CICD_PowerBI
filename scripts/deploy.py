#!/usr/bin/env python3
"""
Script de déploiement Power BI vers Microsoft Fabric
Supporte le déploiement complet et incrémental
"""

import argparse
import json
import os
import sys
import glob
from pathlib import Path
from typing import List, Set, Dict, Optional
import yaml

from utils import (
    get_access_token_spn,
    create_or_update_item_from_folder,
    list_items_by_type,
    deploy_report_via_fabric_workaround,
    find_dataset_cross_workspace,
    rebind_report_cross_workspace,
)


def load_workspace_mapping(mapping_file: str = "workspace-mapping.yml") -> dict:
    """Charge le fichier de mapping des artefacts vers les workspaces."""
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
    type_key = "semanticmodel" if item_type == "SemanticModel" else "report"

    if artifact_name in mapping:
        artifact_config = mapping[artifact_name]

        # Nouveau format avec séparation semanticmodel/report
        if type_key in artifact_config:
            workspace_id = artifact_config[type_key].get(environment)
            if workspace_id:
                print(f"✅ Workspace trouvé pour '{artifact_name}' [{item_type}] ({environment}): {workspace_id}")
                return workspace_id

        # Ancien format (rétrocompatibilité)
        elif environment in artifact_config:
            workspace_id = artifact_config.get(environment)
            if workspace_id:
                print(f"✅ Workspace trouvé (format legacy) pour '{artifact_name}' ({environment}): {workspace_id}")
                return workspace_id

    # Fallback sur le workspace par défaut
    if "default" in mapping:
        default_config = mapping["default"]

        if type_key in default_config:
            workspace_id = default_config[type_key].get(environment)
            if workspace_id:
                print(f"⚠️ Utilisation du workspace par défaut [{item_type}] pour '{artifact_name}': {workspace_id}")
                return workspace_id

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
                "dataset_name": artifact_name
            }

    # Fallback 2: format legacy
    if environment in artifact_config:
        workspace_id = artifact_config.get(environment)
        if workspace_id:
            print(f"📍 Fallback (legacy): Dataset dans le même workspace")
            return {
                "workspace_id": workspace_id,
                "dataset_name": artifact_name
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
        dataset_id=dataset_id
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

    return report_id


def parse_arguments():
    """Parse les arguments en ligne de commande"""
    parser = argparse.ArgumentParser(description='Deploy Power BI to Fabric')
    parser.add_argument(
        '--env',
        required=True,
        choices=['dev', 'prp', 'prd'],
        help='Environnement cible'
    )
    parser.add_argument(
        '--changed-files',
        type=str,
        help='JSON array des fichiers modifiés (pour déploiement incrémental)'
    )
    parser.add_argument(
        '--workspace-mapping',
        default='workspace-mapping.yml',
        help='Fichier de mapping des workspaces'
    )
    return parser.parse_args()


def extract_artifacts_from_changed_files(changed_files: List[str]) -> Dict[str, Set[str]]:
    """
    Extrait les noms des artifacts depuis la liste des fichiers modifiés
    """
    artifacts = {
        'semanticmodels': set(),
        'reports': set()
    }
    
    for file_path in changed_files:
        path = Path(file_path)
        
        if not str(path).startswith('src/'):
            continue
        
        parts = path.parts
        if len(parts) < 2:
            continue
        
        artifact_folder = parts[1]
        
        if artifact_folder.endswith('.Report'):
            artifact_name = artifact_folder.replace('.Report', '')
            artifacts['reports'].add(artifact_name)
            print(f"  📊 Rapport détecté: {artifact_name}")
            
        elif artifact_folder.endswith('.SemanticModel'):
            artifact_name = artifact_folder.replace('.SemanticModel', '')
            artifacts['semanticmodels'].add(artifact_name)
            print(f"  📈 Dataset détecté: {artifact_name}")
    
    return artifacts


def get_all_artifacts(src_dir: Path = Path('src')) -> Dict[str, Set[str]]:
    """
    Récupère tous les artifacts disponibles dans le dossier src/
    """
    artifacts = {
        'semanticmodels': set(),
        'reports': set()
    }
    
    if not src_dir.exists():
        print(f"❌ Le dossier {src_dir} n'existe pas")
        return artifacts
    
    for item in src_dir.iterdir():
        if not item.is_dir():
            continue
        
        if item.name.endswith('.Report'):
            artifact_name = item.name.replace('.Report', '')
            artifacts['reports'].add(artifact_name)
            
        elif item.name.endswith('.SemanticModel'):
            artifact_name = item.name.replace('.SemanticModel', '')
            artifacts['semanticmodels'].add(artifact_name)
    
    return artifacts


def deploy_artifacts(
    environment: str,
    artifacts_to_deploy: Dict[str, Set[str]],
    workspace_mapping: dict,
    token: str,
    is_incremental: bool = False
):
    """
    Déploie les artifacts vers Fabric
    """
    mode = "INCRÉMENTAL" if is_incremental else "COMPLET"
    print(f"\n{'='*60}")
    print(f"🚀 DÉPLOIEMENT {mode} vers {environment.upper()}")
    print(f"{'='*60}\n")
    
    # 1. Déployer les SemanticModels
    semanticmodels = artifacts_to_deploy['semanticmodels']
    if semanticmodels:
        print(f"\n📈 Déploiement de {len(semanticmodels)} SemanticModel(s):")
        for sm_name in sorted(semanticmodels):
            print(f"  → {sm_name}")
            
            try:
                folder = f"src/{sm_name}.SemanticModel"
                
                workspace_id = get_workspace_for_artifact(
                    artifact_name=sm_name,
                    item_type="SemanticModel",
                    environment=environment,
                    mapping=workspace_mapping
                )

                create_or_update_item_from_folder(
                    workspace_id=workspace_id,
                    folder=folder,
                    item_type="SemanticModel",
                    token=token,
                )
            except Exception as e:
                print(f"❌ Échec pour {sm_name}: {e}")
                continue
    else:
        print("\n📈 Aucun SemanticModel à déployer")
    
    # 2. Déployer les Reports
    reports = artifacts_to_deploy['reports']
    if reports:
        print(f"\n📊 Déploiement de {len(reports)} Report(s):")
        for report_name in sorted(reports):
            print(f"  → {report_name}")
            
            try:
                folder = f"src/{report_name}.Report"
                
                report_workspace_id = get_workspace_for_artifact(
                    artifact_name=report_name,
                    item_type="Report",
                    environment=environment,
                    mapping=workspace_mapping
                )

                deploy_report_with_cross_workspace_dataset(
                    report_workspace_id=report_workspace_id,
                    folder=folder,
                    artifact_name=report_name,
                    environment=environment,
                    workspace_mapping=workspace_mapping,
                    token=token
                )
            except Exception as e:
                print(f"❌ Échec pour {report_name}: {e}")
                continue
    else:
        print("\n📊 Aucun Report à déployer")
    
    print(f"\n{'='*60}")
    print(f"✅ Déploiement {mode} terminé avec succès !")
    print(f"{'='*60}\n")


def main():
    """Point d'entrée principal"""
    args = parse_arguments()
    
    # Authentification
    print("🔐 Authentification...")
    token = get_access_token_spn()
    print("✅ Authentification réussie\n")
    
    # Charger le mapping
    print(f"📋 Chargement du mapping: {args.workspace_mapping}")
    workspace_mapping = load_workspace_mapping(args.workspace_mapping)
    print(f"✅ {len([k for k in workspace_mapping.keys() if k != 'default'])} artefacts mappés\n")
    
    # Déterminer le mode de déploiement
    if args.changed_files:
        # Mode incrémental
        print("Mode: Déploiement incrémental\n")
        print("🔍 Analyse des fichiers modifiés...")
        changed_files = json.loads(args.changed_files)
        
        if not changed_files:
            print("✅ Aucun fichier modifié - Déploiement annulé")
            return 0
        
        print(f"Fichiers modifiés: {len(changed_files)}")
        for file in changed_files:
            print(f"  • {file}")
        
        artifacts_to_deploy = extract_artifacts_from_changed_files(changed_files)
        
        total_artifacts = len(artifacts_to_deploy['reports']) + len(artifacts_to_deploy['semanticmodels'])
        if total_artifacts == 0:
            print("\n⚠️  Aucun artifact Power BI détecté dans les changements")
            return 0
        
        deploy_artifacts(args.env, artifacts_to_deploy, workspace_mapping, token, is_incremental=True)
     
        
    else:
        # Mode complet
        print("Mode: Déploiement complet\n")
        print("📦 Scan de tous les artifacts...")
        artifacts_to_deploy = get_all_artifacts()
        
        total_artifacts = len(artifacts_to_deploy['reports']) + len(artifacts_to_deploy['semanticmodels'])
        if total_artifacts == 0:
            print("⚠️  Aucun artifact trouvé dans src/")
            return 0
        
        print(f"Artifacts trouvés:")
        print(f"  • {len(artifacts_to_deploy['semanticmodels'])} SemanticModel(s)")
        print(f"  • {len(artifacts_to_deploy['reports'])} Report(s)")
        
        deploy_artifacts(args.env, artifacts_to_deploy, workspace_mapping, token, is_incremental=False)
       
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
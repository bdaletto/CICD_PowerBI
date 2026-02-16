#!/usr/bin/env python3
"""
Script de déploiement Power BI vers Microsoft Fabric
Supporte le déploiement complet et incrémental
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Set, Dict, Optional

# Import de tes modules existants (à adapter selon ta structure)
# from utils import get_access_token_spn, load_workspace_mapping, load_parameters_mapping
# from deploy_semanticmodel import deploy_semantic_model
# from deploy_report import deploy_report


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
    return parser.parse_args()


def extract_artifacts_from_changed_files(changed_files: List[str]) -> Dict[str, Set[str]]:
    """
    Extrait les noms des artifacts (Reports et SemanticModels) depuis la liste des fichiers modifiés
    
    Args:
        changed_files: Liste des chemins de fichiers modifiés (ex: ['src/Mon Rapport.Report/definition.pbir'])
    
    Returns:
        Dict avec 'reports' et 'semanticmodels' contenant les noms des artifacts à déployer
    """
    artifacts = {
        'semanticmodels': set(),
        'reports': set()
    }
    
    for file_path in changed_files:
        # Normaliser le chemin
        path = Path(file_path)
        
        # Ignorer les fichiers hors de src/
        if not str(path).startswith('src/'):
            continue
        
        # Extraire les parties du chemin : src/Mon Rapport.Report/definition.pbir
        parts = path.parts
        
        if len(parts) < 2:
            continue
        
        # Le nom de l'artifact est le dossier juste après 'src/'
        artifact_folder = parts[1]  # Ex: "Mon Rapport.Report" ou "Mon Dataset.SemanticModel"
        
        # Déterminer le type d'artifact
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
    
    Returns:
        Dict avec 'reports' et 'semanticmodels' contenant tous les noms d'artifacts
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
    is_incremental: bool = False
):
    """
    Déploie les artifacts vers Fabric
    
    Args:
        environment: dev, prp ou prd
        artifacts_to_deploy: Dict avec les listes de reports et semanticmodels à déployer
        is_incremental: Si True, indique qu'il s'agit d'un déploiement incrémental
    """
    mode = "INCRÉMENTAL" if is_incremental else "COMPLET"
    print(f"\n{'='*60}")
    print(f"🚀 DÉPLOIEMENT {mode} vers {environment.upper()}")
    print(f"{'='*60}\n")
    
    # Importer tes fonctions de déploiement
    # from utils import get_access_token_spn, load_workspace_mapping, load_parameters_mapping
    # from deploy_semanticmodel import deploy_semantic_model
    # from deploy_report import deploy_report
    
    # 1. Authentification
    print("🔐 Authentification...")
    # token = get_access_token_spn()
    # workspace_mapping = load_workspace_mapping()
    # parameters_mapping = load_parameters_mapping()
    
    # 2. Déploiement des SemanticModels
    semanticmodels = artifacts_to_deploy['semanticmodels']
    if semanticmodels:
        print(f"\n📈 Déploiement de {len(semanticmodels)} SemanticModel(s):")
        for sm_name in sorted(semanticmodels):
            print(f"  → {sm_name}")
            # deploy_semantic_model(
            #     name=sm_name,
            #     environment=environment,
            #     token=token,
            #     workspace_mapping=workspace_mapping,
            #     parameters_mapping=parameters_mapping
            # )
    else:
        print("\n📈 Aucun SemanticModel à déployer")
    
    # 3. Déploiement des Reports
    reports = artifacts_to_deploy['reports']
    if reports:
        print(f"\n📊 Déploiement de {len(reports)} Report(s):")
        for report_name in sorted(reports):
            print(f"  → {report_name}")
            # deploy_report(
            #     name=report_name,
            #     environment=environment,
            #     token=token,
            #     workspace_mapping=workspace_mapping
            # )
    else:
        print("\n📊 Aucun Report à déployer")
    
    print(f"\n{'='*60}")
    print(f"✅ Déploiement {mode} terminé avec succès !")
    print(f"{'='*60}\n")


def main():
    """Point d'entrée principal"""
    args = parse_arguments()
    
    # Déterminer le mode de déploiement
    if args.changed_files:
        # Mode incrémental
        print("\n🔍 Analyse des fichiers modifiés...")
        changed_files = json.loads(args.changed_files)
        
        if not changed_files:
            print("✅ Aucun fichier modifié - Déploiement annulé")
            return 0
        
        print(f"Fichiers modifiés: {len(changed_files)}")
        for file in changed_files:
            print(f"  • {file}")
        
        artifacts_to_deploy = extract_artifacts_from_changed_files(changed_files)
        
        # Vérifier s'il y a des artifacts à déployer
        total_artifacts = len(artifacts_to_deploy['reports']) + len(artifacts_to_deploy['semanticmodels'])
        if total_artifacts == 0:
            print("\n⚠️  Aucun artifact Power BI détecté dans les changements")
            print("Les fichiers modifiés ne correspondent pas à des Reports ou SemanticModels")
            return 0
        
        deploy_artifacts(args.env, artifacts_to_deploy, is_incremental=True)
        
    else:
        # Mode complet
        print("\n📦 Déploiement complet de tous les artifacts...")
        artifacts_to_deploy = get_all_artifacts()
        
        total_artifacts = len(artifacts_to_deploy['reports']) + len(artifacts_to_deploy['semanticmodels'])
        if total_artifacts == 0:
            print("⚠️  Aucun artifact trouvé dans src/")
            return 0
        
        print(f"Artifacts trouvés:")
        print(f"  • {len(artifacts_to_deploy['semanticmodels'])} SemanticModel(s)")
        print(f"  • {len(artifacts_to_deploy['reports'])} Report(s)")
        
        deploy_artifacts(args.env, artifacts_to_deploy, is_incremental=False)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
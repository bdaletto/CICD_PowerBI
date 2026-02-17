#!/usr/bin/env python3
"""
Validation des paramètres Power Query dans le mapping workspace
Exécuté lors des Pull Requests via le workflow BPA
"""

import argparse
import os
import re
import sys
import glob
import yaml
from pathlib import Path


def load_workspace_mapping(mapping_file: str) -> dict:
    if not os.path.exists(mapping_file):
        raise FileNotFoundError(f"Fichier de mapping introuvable: {mapping_file}")
    with open(mapping_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def detect_parameters_in_expressions(expressions_path: str) -> list:
    """
    Détecte les paramètres M (IsParameterQuery=true) dans expressions.tmdl
    """
    with open(expressions_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    return re.findall(
        r'expression\s+(\w+)\s*=\s*"[^"]*"\s*meta\s*\[IsParameterQuery=true',
        content
    )


def validate_semantic_model(
    folder: str,
    artifact_name: str,
    mapping: dict,
    environments: list = ["dev", "prp", "prd"]
) -> list:
    """
    Valide les paramètres d'un SemanticModel.
    Retourne une liste d'erreurs (vide = tout OK)
    """
    errors = []
    expressions_path = os.path.join(folder, "definition", "expressions.tmdl")

    # Pas de expressions.tmdl = Pattern B ou pas de paramètres, on skip
    if not os.path.exists(expressions_path):
        return errors
    
    detected_params = detect_parameters_in_expressions(expressions_path)
    
    # Pas de paramètres M = rien à valider
    if not detected_params:
        return errors

    print(f"\n[SEARCH] [{artifact_name}] Parametres detectes : {detected_params}")

    artifact_config = mapping.get(artifact_name, {})

    # Artifact absent du mapping
    if not artifact_config:
        errors.append({
            "artifact": artifact_name,
            "level": "CRITICAL",
            "message": f"Artifact absent de workspace-mapping.yml",
            "params": detected_params,
            "suggestion": f"""
    {artifact_name}:
      semanticmodel:
        dev: "<workspace-id-dev>"
        prp: "<workspace-id-prp>"
        prd: "<workspace-id-prd>"
      parameters:
        dev:
{chr(10).join(f'          {p}: "<valeur-dev>"' for p in detected_params)}
        prp:
{chr(10).join(f'          {p}: "<valeur-prp>"' for p in detected_params)}
        prd:
{chr(10).join(f'          {p}: "<valeur-prd>"' for p in detected_params)}"""
        })
        return errors

    # Section parameters absente
    if "parameters" not in artifact_config:
        errors.append({
            "artifact": artifact_name,
            "level": "CRITICAL",
            "message": "Section 'parameters' manquante dans workspace-mapping.yml",
            "params": detected_params,
            "suggestion": f"""
      parameters:
        dev:
{chr(10).join(f'          {p}: "<valeur-dev>"' for p in detected_params)}
        prp:
{chr(10).join(f'          {p}: "<valeur-prp>"' for p in detected_params)}
        prd:
{chr(10).join(f'          {p}: "<valeur-prd>"' for p in detected_params)}"""
        })
        return errors

    parameters_config = artifact_config["parameters"]

    # Vérifier chaque environnement
    for env in environments:
        
        if env not in parameters_config:
            errors.append({
                "artifact": artifact_name,
                "level": "ERROR",
                "message": f"Environnement '{env}' absent de la section parameters",
                "suggestion": f"""
        {env}:
{chr(10).join(f'          {p}: "<valeur-{env}>"' for p in detected_params)}"""
            })
            continue

        env_params = parameters_config[env]
        missing = [p for p in detected_params if p not in env_params]
        empty   = [p for p in detected_params if p in env_params and not env_params[p]]

        if missing:
            errors.append({
                "artifact": artifact_name,
                "level": "ERROR",
                "message": f"[{env.upper()}] Parametres absents : {missing}",
                "suggestion": f"""
        {env}:
{chr(10).join(f'          {p}: "<valeur-{env}>"' for p in missing)}"""
            })

        if empty:
            errors.append({
                "artifact": artifact_name,
                "level": "WARNING",
                "message": f"[{env.upper()}] Parametres vides : {empty}",
                "suggestion": f"Renseigne les valeurs manquantes pour : {empty}"
            })

        if not missing and not empty:
            print(f"   [OK] [{env.upper()}] Tous les parametres sont configures")

    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Valide les parametres Power Query dans workspace-mapping.yml"
    )
    parser.add_argument("--src", default="src", help="Dossier source")
    parser.add_argument("--mapping", default="workspace-mapping.yml", help="Fichier de mapping")
    args = parser.parse_args()

    print("=" * 60)
    print("[SEARCH] VALIDATION DES PARAMETRES POWER QUERY")
    print("=" * 60)

    mapping = load_workspace_mapping(args.mapping)

    # Scanner tous les SemanticModels
    sm_folders = glob.glob(os.path.join(args.src, "*.SemanticModel"))

    if not sm_folders:
        print("[INFO] Aucun SemanticModel trouve -- rien a valider")
        sys.exit(0)

    all_errors   = []
    all_warnings = []

    for folder in sorted(sm_folders):
        artifact_name = Path(folder).stem
        errors = validate_semantic_model(folder, artifact_name, mapping)

        for e in errors:
            if e["level"] == "WARNING":
                all_warnings.append(e)
            else:
                all_errors.append(e)

    # Rapport final
    print(f"\n{'=' * 60}")
    print(f"[REPORT] RAPPORT DE VALIDATION")
    print(f"{'=' * 60}")

    if all_warnings:
        print(f"\n[WARNING] {len(all_warnings)} WARNING(S) :")
        for w in all_warnings:
            print(f"""
   [{w['artifact']}] {w['message']}
   [TIP] {w['suggestion']}""")

    if all_errors:
        print(f"\n[ERROR] {len(all_errors)} ERREUR(S) BLOQUANTE(S) :")
        for e in all_errors:
            print(f"""
   ============================================================
   [{e['artifact']}] {e['message']}
   ------------------------------------------------------------
   [ACTION] Ajoute dans workspace-mapping.yml :
{e['suggestion']}
   ============================================================""")
        
        print(f"""
{'=' * 60}
[ERROR] VALIDATION ECHOUEE -- PR bloquee
{'=' * 60}
Corrige workspace-mapping.yml avant de merger cette PR.
""")
        sys.exit(1)

    print(f"\n[OK] Validation reussie -- tous les parametres sont configures\n")
    sys.exit(0)


if __name__ == "__main__":
    main()
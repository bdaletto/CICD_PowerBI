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


# ============================================================
# DETECTION DES SOURCES HARDCODÉES
# ============================================================

# Fonctions M connecteur connues — liste non exhaustive, complétée par détection générique
# Format : NomFonction -> description lisible
KNOWN_CONNECTOR_FUNCTIONS = {
    "Sql.Database": "SQL Server",
    "Sql.Databases": "SQL Server",
    "PostgreSQL.Database": "PostgreSQL",
    "MySQL.Database": "MySQL",
    "Oracle.Database": "Oracle",
    "Teradata.Database": "Teradata",
    "Snowflake.Databases": "Snowflake",
    "GoogleBigQuery.Database": "BigQuery",
    "AzureSynapse.Database": "Azure Synapse",
    "AzureDataLake.Storage": "ADLS",
    "AzureBlobStorage.Contents": "Azure Blob",
    "AzureDataExplorer.Contents": "Azure Data Explorer",
    "Databricks.Catalogs": "Databricks",
    "Databricks.Contents": "Databricks",
    "SapHana.Database": "SAP HANA",
    "DB2.Database": "IBM DB2",
    "Sybase.Database": "Sybase",
    "MongoDb.Find": "MongoDB",
    "OData.Feed": "OData",
    "Web.Contents": "Web",
    "File.Contents": "File",
    "Folder.Files": "Folder",
    "SharePoint.Files": "SharePoint",
    "SharePoint.Tables": "SharePoint",
    "Exchange.Contents": "Exchange",
    "Salesforce.Data": "Salesforce",
    "Salesforce.Reports": "Salesforce",
}

# Pattern générique pour détecter n'importe quelle fonction M de type connecteur
# Xxx.Yyy(...) où le premier argument est une string littérale
_GENERIC_CONNECTOR_PATTERN = re.compile(
    r'\b([A-Z][A-Za-z0-9]*\.[A-Za-z][A-Za-z0-9]*)\s*\(([^)]*)\)',
    re.MULTILINE
)

# Pattern pour extraire les arguments string littéraux d'une fonction
_STRING_LITERAL = re.compile(r'^"([^"]+)"')

# Pattern SQL FROM — couvre :
#   BigQuery backtick:  FROM `project.dataset.table` ou `project`.`dataset`.`table`
#   SQL Server bracket: FROM [server].[db].[schema].[table]
#   Standard quoted:    FROM "schema"."table"
#   Standard bare:      FROM schema.table  (au moins un point)
_SQL_FROM_HARDCODED = re.compile(
    r'\bFROM\s+'
    r'('
    r'`[^`]+(?:`\s*\.\s*`[^`]+)*`'   # backtick GCP
    r'|'
    r'\[[^\]]+\](?:\s*\.\s*\[[^\]]+\])*'  # bracket SQL Server
    r'|'
    r'"[^"]+"(?:\s*\.\s*"[^"]+")*'   # double-quoted
    r'|'
    r'[a-zA-Z_][a-zA-Z0-9_-]*\.[a-zA-Z_][a-zA-Z0-9_.`\[\]-]*'  # bare dotted identifier
    r')',
    re.IGNORECASE | re.MULTILINE
)

# Pattern pour repérer Value.NativeQuery et extraire la requête SQL
_NATIVE_QUERY = re.compile(
    r'Value\.NativeQuery\s*\([^,)]+,\s*"((?:[^"\\]|\\.)*)"',
    re.DOTALL
)


def _first_arg_is_literal(args_str: str) -> tuple[bool, str]:
    """
    Retourne (True, valeur) si le premier argument est une string littérale,
    (False, "") si c'est un identifiant (= référence à un paramètre ou variable).
    """
    stripped = args_str.strip()
    if not stripped:
        return False, ""
    m = _STRING_LITERAL.match(stripped)
    if m:
        return True, m.group(1)
    return False, ""


def _extract_query_name(content: str, pos: int) -> str:
    """
    Remonte dans le contenu pour trouver le nom de la query/expression M
    qui contient la position donnée.
    """
    before = content[:pos]
    # Cherche le dernier bloc 'expression NomTable = ...' avant la position
    matches = list(re.finditer(r'^\s*(?:expression|table)\s+([^\s=\r\n]+)', before, re.MULTILINE))
    if matches:
        return matches[-1].group(1).strip("'\"")
    return "<inconnu>"


def detect_hardcoded_sources_in_file(tmdl_path: str) -> list:
    """
    Analyse un fichier .tmdl et retourne une liste de findings :
    {
        "query": nom de la query M,
        "type": "connector" | "sql_from",
        "function": nom de la fonction (si connector),
        "value": la valeur hardcodée détectée,
        "line": numéro de ligne approx,
    }
    """
    with open(tmdl_path, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    findings = []
    lines = content.split('\n')

    def line_of(pos):
        return content[:pos].count('\n') + 1

    # --- 1. Détection des fonctions connecteur avec littéral ---
    for m in _GENERIC_CONNECTOR_PATTERN.finditer(content):
        func_name = m.group(1)
        args_str = m.group(2)

        # Filtrer : on ne garde que les fonctions qui ressemblent à des connecteurs
        # (présentes dans la liste connue OU dont le nom commence par une majuscule
        # et contient un point — heuristique raisonnable pour M)
        is_known = func_name in KNOWN_CONNECTOR_FUNCTIONS
        looks_like_connector = bool(re.match(r'^[A-Z][A-Za-z0-9]+\.[A-Za-z]', func_name))

        if not (is_known or looks_like_connector):
            continue

        # Exclure les fonctions purement utilitaires non liées aux sources
        excluded_prefixes = ('Table.', 'List.', 'Record.', 'Text.', 'Number.',
                             'Date.', 'DateTime.', 'Duration.', 'Json.', 'Xml.',
                             'Binary.', 'Value.', 'Type.', 'Logical.', 'Error.',
                             'Exception.', 'Function.', 'Expression.', 'Lines.',
                             'Csv.', 'Html.', 'Pdf.')
        if any(func_name.startswith(p) for p in excluded_prefixes):
            continue

        is_literal, literal_value = _first_arg_is_literal(args_str)
        if not is_literal:
            continue  # C'est une référence à un paramètre → OK

        query_name = _extract_query_name(content, m.start())
        connector_label = KNOWN_CONNECTOR_FUNCTIONS.get(func_name, func_name)

        findings.append({
            "query": query_name,
            "type": "connector",
            "function": func_name,
            "connector_label": connector_label,
            "value": literal_value,
            "line": line_of(m.start()),
        })

    # --- 2. Détection des FROM hardcodés dans Value.NativeQuery ---
    for nq_match in _NATIVE_QUERY.finditer(content):
        sql = nq_match.group(1).replace('\\n', '\n').replace('\\"', '"')
        query_name = _extract_query_name(content, nq_match.start())

        for from_match in _SQL_FROM_HARDCODED.finditer(sql):
            findings.append({
                "query": query_name,
                "type": "sql_from",
                "function": "Value.NativeQuery",
                "connector_label": "Native SQL",
                "value": from_match.group(1).strip(),
                "line": line_of(nq_match.start()),
            })

    # --- 3. Détection des FROM hardcodés dans les strings SQL inline ---
    # Cherche toute string entre guillemets qui contient une clause FROM
    _INLINE_SQL = re.compile(r'"((?:[^"\\]|\\.)*?\bFROM\b(?:[^"\\]|\\.)*?)"', re.IGNORECASE | re.DOTALL)
    for sql_match in _INLINE_SQL.finditer(content):
        sql = sql_match.group(1).replace('\\n', '\n').replace('\\"', '"')
        query_name = _extract_query_name(content, sql_match.start())

        for from_match in _SQL_FROM_HARDCODED.finditer(sql):
            value = from_match.group(1).strip()
            # Dédoublonner avec ce qu'on a déjà trouvé via NativeQuery
            already = any(
                f["type"] == "sql_from"
                and f["query"] == query_name
                and f["value"] == value
                for f in findings
            )
            if not already:
                findings.append({
                    "query": query_name,
                    "type": "sql_from",
                    "function": "inline SQL",
                    "connector_label": "Inline SQL",
                    "value": value,
                    "line": line_of(sql_match.start()),
                })

    return findings


def detect_hardcoded_sources_in_model(folder: str) -> dict:
    """
    Scanne tous les fichiers .tmdl du dossier definition/ d'un SemanticModel.
    Retourne un dict { fichier: [findings] }
    """
    definition_dir = os.path.join(folder, "definition")
    results = {}

    if not os.path.isdir(definition_dir):
        return results

    tmdl_files = glob.glob(os.path.join(definition_dir, "**", "*.tmdl"), recursive=True)

    for tmdl_path in sorted(tmdl_files):
        findings = detect_hardcoded_sources_in_file(tmdl_path)
        if findings:
            rel_path = os.path.relpath(tmdl_path, folder)
            results[rel_path] = findings

    return results


# ============================================================
# VALIDATION DES PARAMÈTRES (existante)
# ============================================================

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

    if not os.path.exists(expressions_path):
        return errors
    
    detected_params = detect_parameters_in_expressions(expressions_path)
    
    if not detected_params:
        return errors

    print(f"\n[SEARCH] [{artifact_name}] Parametres detectes : {detected_params}")

    artifact_config = mapping.get(artifact_name, {})

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


# ============================================================
# MAIN
# ============================================================

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

    sm_folders = glob.glob(os.path.join(args.src, "*.SemanticModel"))

    if not sm_folders:
        print("[INFO] Aucun SemanticModel trouve -- rien a valider")
        sys.exit(0)

    all_errors   = []
    all_warnings = []
    all_hardcoded_warnings = []  # Warnings sources hardcodées (non bloquant)

    for folder in sorted(sm_folders):
        artifact_name = Path(folder).stem

        # --- Validation paramètres (existante) ---
        errors = validate_semantic_model(folder, artifact_name, mapping)
        for e in errors:
            if e["level"] == "WARNING":
                all_warnings.append(e)
            else:
                all_errors.append(e)

        # --- Détection sources hardcodées (nouveau) ---
        print(f"\n[SEARCH] [{artifact_name}] Scan des sources hardcodees...")
        hardcoded = detect_hardcoded_sources_in_model(folder)

        if not hardcoded:
            print(f"   [OK] Aucune source hardcodee detectee")
        else:
            total = sum(len(v) for v in hardcoded.values())
            print(f"   [WARNING] {total} source(s) hardcodee(s) detectee(s) dans {len(hardcoded)} fichier(s)")
            all_hardcoded_warnings.append({
                "artifact": artifact_name,
                "findings_by_file": hardcoded
            })

    # ============================================================
    # RAPPORT FINAL
    # ============================================================
    print(f"\n{'=' * 60}")
    print(f"[REPORT] RAPPORT DE VALIDATION")
    print(f"{'=' * 60}")

    # --- Warnings paramètres vides ---
    if all_warnings:
        print(f"\n[WARNING] {len(all_warnings)} WARNING(S) parametres :")
        for w in all_warnings:
            print(f"""
   [{w['artifact']}] {w['message']}
   [TIP] {w['suggestion']}""")

    # --- Warnings sources hardcodées ---
    if all_hardcoded_warnings:
        print(f"\n{'=' * 60}")
        print(f"[WARNING] SOURCES HARDCODEES DETECTEES (non bloquant)")
        print(f"{'=' * 60}")
        print(f"Ces sources ne referencent pas de parametres M.")
        print(f"Envisage de les parametriser pour faciliter les deployments multi-env.\n")

        for model_warn in all_hardcoded_warnings:
            artifact_name = model_warn["artifact"]
            print(f"\n  [{artifact_name}]")
            print(f"  {'─' * 50}")

            for file_rel, findings in model_warn["findings_by_file"].items():
                print(f"  Fichier : {file_rel}")

                for f in findings:
                    if f["type"] == "connector":
                        print(f"    ⚠  Ligne {f['line']:>4} | {f['connector_label']:<20} | {f['function']}(\"{f['value']}\")")
                    else:
                        print(f"    ⚠  Ligne {f['line']:>4} | {f['connector_label']:<20} | FROM {f['value']}")

                print()

    # --- Erreurs bloquantes paramètres ---
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
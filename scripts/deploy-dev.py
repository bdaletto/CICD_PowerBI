import argparse
from pathlib import Path

from utils import (
    get_access_token_spn,
    get_or_create_workspace,
    create_or_update_item_from_folder,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deploy PBIP Report & SemanticModel to DEV workspace using Fabric REST APIs."
    )
    parser.add_argument(
        "--workspace",
        required=True,
        help="Fabric workspace name (DEV)",
    )
    parser.add_argument(
        "--capacity",
        default="",
        help="(Optional) Capacity ID to assign the workspace to.",
    )
    parser.add_argument(
        "--admin-upns",
        default="",
        help="(Unused for now) Admin UPNs – kept for compatibility.",
    )

    args = parser.parse_args()

    print("=== DEPLOY TO DEV ===")

    # 1. Auth SPN -> token
    print("Authenticating with Service Principal (client_credentials)...")
    token = get_access_token_spn()
    print("SPN authentication successful.")

    # 2. Workspace DEV
    ws_id = get_or_create_workspace(
        workspace_name=args.workspace,
        token=token,
        capacity_id=args.capacity or None,
    )
    print(f"Using workspace '{args.workspace}' (id={ws_id})")

    # 3. Deploy Semantic Models (*.SemanticModel in src/)
    # rglob("*.SemanticModel/*") matches files inside .SemanticModel directories
    # Extract unique parent directories (the .SemanticModel folders themselves)
    semantic_paths = list(Path("src").rglob("*.SemanticModel/*"))
    semantic_folders = set()
    for p in semantic_paths:
        if p.is_file():
            # Walk up the path to find the directory ending with .SemanticModel
            current = p.parent
            while current != Path("src").parent and current != Path("."):
                if current.name.endswith(".SemanticModel"):
                    semantic_folders.add(current)
                    break
                current = current.parent
    semantic_folders = sorted(semantic_folders)
    
    if not semantic_folders:
        print("No *.SemanticModel folders found under src/ – skipping semantic models.")
    else:
        for folder in semantic_folders:
            create_or_update_item_from_folder(
                workspace_id=ws_id,
                folder=str(folder),
                item_type="SemanticModel",
                token=token,
            )

    # 4. Deploy Reports (*.Report in src/)
    # rglob("*.Report/*") matches files inside .Report directories
    # Extract unique parent directories (the .Report folders themselves)
    report_paths = list(Path("src").rglob("*.Report/*"))
    report_folders = set()
    for p in report_paths:
        if p.is_file():
            # Walk up the path to find the directory ending with .Report
            current = p.parent
            while current != Path("src").parent and current != Path("."):
                if current.name.endswith(".Report"):
                    report_folders.add(current)
                    break
                current = current.parent
    report_folders = sorted(report_folders)
    
    if not report_folders:
        print("No *.Report folders found under src/ – skipping reports.")
    else:
        for folder in report_folders:
            create_or_update_item_from_folder(
                workspace_id=ws_id,
                folder=str(folder),
                item_type="Report",
                token=token,
            )

    print("\nDEV deployment finished successfully.")


if __name__ == "__main__":
    main()
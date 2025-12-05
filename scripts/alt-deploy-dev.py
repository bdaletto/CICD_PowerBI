import argparse
import glob
import os
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
import argparse
import glob
import os

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

    # Initialize the FabricWorkspace object with the required parameters
    target_workspace = FabricWorkspace(
        workspace_id = ws_id,
        #environment = "DEV",
        repository_directory = "**/src",
        item_type_in_scope = ["Report", "SemanticModel"],
    )

    # Publish all items defined in item_type_in_scope
    publish_all_items(target_workspace)
   

    print("\nDEV deployment finished successfully.")


if __name__ == "__main__":
    main()
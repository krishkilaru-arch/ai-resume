"""
Deploy Resume Data Model to Databricks

This script uploads the notebook and optionally runs it to create
the resume tables in your Databricks workspace.

Usage:
    # Set environment variables first:
    export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
    export DATABRICKS_TOKEN="dapi..."

    # Deploy notebook only
    python src/deploy.py --upload

    # Deploy and run
    python src/deploy.py --upload --run

    # Deploy with custom catalog/schema
    python src/deploy.py --upload --run --catalog my_catalog --schema my_schema
"""

import argparse
import json
import os
import sys
import base64
import time
from pathlib import Path

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat, Language
    from databricks.sdk.service.jobs import NotebookTask, Task, RunLifeCycleState
except ImportError:
    print("ERROR: databricks-sdk not installed. Run: pip install databricks-sdk")
    sys.exit(1)


PROJECT_ROOT = Path(__file__).parent.parent
NOTEBOOK_PATH = PROJECT_ROOT / "notebooks" / "01_setup_resume_tables.py"
RESUME_DATA_PATH = PROJECT_ROOT / "config" / "resume_data.json"

DEFAULT_WORKSPACE_DIR = "/Users/{username}/resume"


def get_client():
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not host or not token:
        print("ERROR: Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
        print("  export DATABRICKS_HOST='https://your-workspace.cloud.databricks.com'")
        print("  export DATABRICKS_TOKEN='dapi...'")
        sys.exit(1)

    return WorkspaceClient(host=host, token=token)


def upload_notebook(client, workspace_dir):
    """Upload the setup notebook to the Databricks workspace."""
    notebook_content = NOTEBOOK_PATH.read_text()
    target_path = f"{workspace_dir}/01_setup_resume_tables"

    print(f"Uploading notebook to {target_path}...")

    client.workspace.mkdirs(workspace_dir)

    content_b64 = base64.b64encode(notebook_content.encode()).decode()
    client.workspace.import_(
        path=target_path,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        content=content_b64,
        overwrite=True,
    )

    print(f"  Notebook uploaded to: {target_path}")
    return target_path


def upload_data_file(client, workspace_dir, catalog, schema):
    """Upload resume data JSON to a Databricks Volume."""
    if not RESUME_DATA_PATH.exists():
        print(f"  WARNING: {RESUME_DATA_PATH} not found, skipping data upload")
        return None

    volume_path = f"/Volumes/{catalog}/{schema}/raw"
    target_path = f"{volume_path}/resume_data.json"

    print(f"Uploading resume data to {target_path}...")

    try:
        data = RESUME_DATA_PATH.read_bytes()
        client.files.upload(target_path, data, overwrite=True)
        print(f"  Data uploaded to: {target_path}")
        return target_path
    except Exception as e:
        print(f"  Note: Could not upload to Volume ({e}).")
        print(f"  The notebook uses inline data by default, so this is optional.")
        return None


def run_notebook(client, notebook_path, catalog, schema):
    """Run the setup notebook as a job."""
    print(f"\nRunning notebook: {notebook_path}...")

    run = client.jobs.submit(
        run_name="Resume Data Model Setup",
        tasks=[
            Task(
                task_key="setup_tables",
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters={
                        "catalog": catalog,
                        "schema": schema,
                    },
                ),
            )
        ],
    )

    print(f"  Run submitted (run_id: {run.run_id})")
    print(f"  Waiting for completion...")

    while True:
        run_status = client.jobs.get_run(run.run_id)
        state = run_status.state.life_cycle_state

        if state in (RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR):
            break

        print(f"  Status: {state.value}...")
        time.sleep(10)

    result_state = run_status.state.result_state
    if result_state and result_state.value == "SUCCESS":
        print(f"\n  Run completed successfully!")
        print(f"  Tables created in: {catalog}.{schema}")
    else:
        print(f"\n  Run failed with state: {result_state}")
        print(f"  Check the run output at: {run_status.run_page_url}")
        sys.exit(1)

    return run_status


def update_notebook_config(catalog, schema):
    """Update the catalog/schema in the notebook before uploading."""
    content = NOTEBOOK_PATH.read_text()
    content = content.replace(
        'CATALOG = "resume_catalog"',
        f'CATALOG = "{catalog}"'
    )
    content = content.replace(
        'SCHEMA = "career_profile"',
        f'SCHEMA = "{schema}"'
    )
    NOTEBOOK_PATH.write_text(content)
    print(f"  Updated notebook config: {catalog}.{schema}")


def create_genie_space_reminder(catalog, schema):
    """Print instructions for creating the Genie Space."""
    print("\n" + "=" * 60)
    print("  NEXT STEPS")
    print("=" * 60)
    print(f"""
  1. GENIE SPACE SETUP:
     - Go to your Databricks workspace
     - Navigate to AI/BI > Genie (left sidebar)
     - Click 'New Genie Space'
     - Name: '[Your Name]'s Interactive Resume'
     - Select tables from: {catalog}.{schema}
     - Add ALL 9 tables
     - Paste instructions from: genie/genie_space_instructions.md
     - Add sample questions from the same file

  2. DASHBOARD SETUP:
     - Go to AI/BI > Dashboards
     - Click 'Create Dashboard'
     - Add widgets using queries from: dashboards/resume_dashboard_queries.sql
     - Each query section has suggested widget types

  3. SHARE:
     - Share your Genie Space URL on your resume/LinkedIn
     - 'Ask my AI resume anything: [URL]'
""")


def main():
    parser = argparse.ArgumentParser(description="Deploy Resume Data Model to Databricks")
    parser.add_argument("--upload", action="store_true", help="Upload notebook to workspace")
    parser.add_argument("--run", action="store_true", help="Run the notebook after uploading")
    parser.add_argument("--catalog", default="resume_catalog", help="Unity Catalog name (default: resume_catalog)")
    parser.add_argument("--schema", default="career_profile", help="Schema name (default: career_profile)")
    parser.add_argument("--workspace-dir", default=None, help="Workspace directory for notebooks")

    args = parser.parse_args()

    if not args.upload and not args.run:
        parser.print_help()
        print("\nExample: python src/deploy.py --upload --run")
        sys.exit(0)

    client = get_client()

    current_user = client.current_user.me()
    username = current_user.user_name
    workspace_dir = args.workspace_dir or DEFAULT_WORKSPACE_DIR.format(username=username)

    print("=" * 60)
    print("  Resume Data Model Deployment")
    print("=" * 60)
    print(f"  Workspace:  {os.environ['DATABRICKS_HOST']}")
    print(f"  User:       {username}")
    print(f"  Catalog:    {args.catalog}")
    print(f"  Schema:     {args.schema}")
    print(f"  Target Dir: {workspace_dir}")
    print("=" * 60 + "\n")

    if args.catalog != "resume_catalog" or args.schema != "career_profile":
        update_notebook_config(args.catalog, args.schema)

    notebook_path = None
    if args.upload:
        notebook_path = upload_notebook(client, workspace_dir)
        upload_data_file(client, workspace_dir, args.catalog, args.schema)

    if args.run:
        if not notebook_path:
            notebook_path = f"{workspace_dir}/01_setup_resume_tables"
        run_notebook(client, notebook_path, args.catalog, args.schema)

    create_genie_space_reminder(args.catalog, args.schema)


if __name__ == "__main__":
    main()

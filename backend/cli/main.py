"""
VTagger CLI.

Command-line interface for VTagger - Virtual Tagging Agent.
Provides commands for managing dimensions, syncing tags,
managing credentials, and running the server.

Ported from BPVtagger with:
- Module name: vtagger (was bpvtagger)
- bizmapping subcommand -> dimensions subcommand
- dimensions list/validate/import/export/resolve commands
- Removed resolve-sysid command (BP-specific)
- VTagger branding throughout
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import click


# ---------------------------------------------------------------------------
# CLI Group
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(version="1.0.0", prog_name="vtagger")
def cli():
    """VTagger - Virtual Tagging Agent.

    Manage virtual tag dimensions, sync with Umbrella, and run the API server.
    """
    pass


# ---------------------------------------------------------------------------
# Dimensions Commands
# ---------------------------------------------------------------------------


@cli.group()
def dimensions():
    """Manage virtual tag dimensions (mapping rules)."""
    pass


@dimensions.command("list")
def dimensions_list():
    """List all loaded dimensions."""
    _ensure_app_context()
    from app.services.mapping_engine import mapping_engine

    mapping_engine.load_dimensions()

    if not mapping_engine.dimensions:
        click.echo("No dimensions loaded.")
        click.echo("Use 'vtagger dimensions import <file>' to import dimensions.")
        return

    click.echo(f"\nLoaded {len(mapping_engine.dimensions)} dimensions:\n")
    click.echo(f"{'#':<4} {'VTag Name':<30} {'Kind':<15} {'Statements':<12} {'Default'}")
    click.echo("-" * 85)

    for dim in mapping_engine._sorted_dimensions:
        stmt_count = len(dim.statements) if dim.statements else 0
        click.echo(
            f"{dim.index:<4} {dim.vtag_name:<30} {dim.kind:<15} "
            f"{stmt_count:<12} {dim.default_value}"
        )

    click.echo()

    # Show required tag keys
    tag_keys = mapping_engine.get_required_tag_keys()
    if tag_keys:
        click.echo(f"Required tag keys ({len(tag_keys)}):")
        for key in sorted(tag_keys):
            click.echo(f"  - {key}")
        click.echo()


@dimensions.command("validate")
@click.argument("file_path", type=click.Path(exists=True))
def dimensions_validate(file_path: str):
    """Validate a dimension JSON file without importing.

    FILE_PATH is the path to a JSON file containing dimension definitions.
    Expected format: a JSON array of dimension objects, or a single dimension object.
    """
    from app.core.dsl_parser import validate_dimension_json

    try:
        with open(file_path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in {file_path}: {e}", err=True)
        sys.exit(1)

    # Normalize to list
    if isinstance(data, dict):
        if "dimensions" in data:
            dims = data["dimensions"]
        else:
            dims = [data]
    elif isinstance(data, list):
        dims = data
    else:
        click.echo("Error: Expected a JSON object or array.", err=True)
        sys.exit(1)

    click.echo(f"Validating {len(dims)} dimension(s) from {file_path}...\n")

    all_valid = True
    for i, dim in enumerate(dims):
        # Ensure vtag_name or vtagName is present
        vtag_name = dim.get("vtag_name") or dim.get("vtagName") or dim.get("name", f"dimension_{i}")

        # Normalize to validation format
        content = {
            "vtag_name": vtag_name,
            "statements": dim.get("statements", []),
        }

        errors = validate_dimension_json(content)
        if errors:
            all_valid = False
            click.echo(f"  [{i}] {vtag_name}: INVALID")
            for err in errors:
                click.echo(f"      - {err}")
        else:
            stmt_count = len(dim.get("statements", []))
            click.echo(f"  [{i}] {vtag_name}: OK ({stmt_count} statements)")

    click.echo()
    if all_valid:
        click.echo("All dimensions are valid.")
    else:
        click.echo("Some dimensions have validation errors.", err=True)
        sys.exit(1)


@dimensions.command("import")
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--replace", is_flag=True, help="Replace existing dimensions with same name.")
def dimensions_import(file_path: str, replace: bool):
    """Import dimensions from a JSON file into the database.

    FILE_PATH is the path to a JSON file containing dimension definitions.
    """
    _ensure_app_context()
    from app.database import execute_query, execute_write
    from app.services.mapping_engine import mapping_engine

    try:
        with open(file_path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in {file_path}: {e}", err=True)
        sys.exit(1)

    # Normalize to list
    if isinstance(data, dict):
        if "dimensions" in data:
            dims = data["dimensions"]
        else:
            dims = [data]
    elif isinstance(data, list):
        dims = data
    else:
        click.echo("Error: Expected a JSON object or array.", err=True)
        sys.exit(1)

    click.echo(f"Importing {len(dims)} dimension(s) from {file_path}...\n")

    imported = 0
    updated = 0
    skipped = 0

    for dim in dims:
        vtag_name = dim.get("vtagName") or dim.get("vtag_name") or dim.get("name", "")
        if not vtag_name:
            click.echo(f"  Skipping dimension without name: {dim}")
            skipped += 1
            continue

        index = dim.get("index", dim.get("index_number", 0))
        kind = dim.get("kind", "TAG_MAPPING")
        default_value = dim.get("defaultValue", dim.get("default_value", "Unallocated"))
        source = dim.get("source", "TAGS")
        statements = dim.get("statements", [])

        # Build content JSON
        content = {
            "vtagName": vtag_name,
            "index": index,
            "kind": kind,
            "defaultValue": default_value,
            "source": source,
            "statements": statements,
        }
        content_json = json.dumps(content)

        # Compute checksum
        import hashlib
        raw = json.dumps(content, sort_keys=True, separators=(",", ":"))
        checksum = hashlib.md5(raw.encode()).hexdigest()

        # Check if exists
        existing = execute_query(
            "SELECT id FROM dimensions WHERE vtag_name = ?", (vtag_name,)
        )

        if existing:
            if replace:
                execute_write(
                    """UPDATE dimensions SET
                    index_number = ?, kind = ?, default_value = ?, source = ?,
                    content = ?, statement_count = ?, checksum = ?,
                    updated_at = CURRENT_TIMESTAMP
                    WHERE vtag_name = ?""",
                    (index, kind, default_value, source, content_json,
                     len(statements), checksum, vtag_name),
                )
                click.echo(f"  Updated: {vtag_name} ({len(statements)} statements)")
                updated += 1
            else:
                click.echo(f"  Skipped (exists): {vtag_name}")
                skipped += 1
        else:
            execute_write(
                """INSERT INTO dimensions
                (vtag_name, index_number, kind, default_value, source,
                 content, statement_count, checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (vtag_name, index, kind, default_value, source,
                 content_json, len(statements), checksum),
            )
            click.echo(f"  Imported: {vtag_name} ({len(statements)} statements)")
            imported += 1

    # Reload mapping engine
    mapping_engine.load_dimensions()

    click.echo(f"\nDone: {imported} imported, {updated} updated, {skipped} skipped.")
    click.echo(f"Total dimensions loaded: {len(mapping_engine.dimensions)}")


@dimensions.command("export")
@click.argument("output_path", type=click.Path(), default="dimensions_export.json")
def dimensions_export(output_path: str):
    """Export dimensions from the database to a JSON file.

    OUTPUT_PATH is the destination file path (default: dimensions_export.json).
    """
    _ensure_app_context()
    from app.database import execute_query

    rows = execute_query(
        "SELECT vtag_name, index_number, kind, default_value, source, content "
        "FROM dimensions ORDER BY index_number"
    )

    if not rows:
        click.echo("No dimensions to export.")
        return

    dims = []
    for row in rows:
        try:
            content = json.loads(row["content"]) if row["content"] else {}
        except (json.JSONDecodeError, TypeError):
            content = {}

        dims.append({
            "vtagName": row["vtag_name"],
            "index": row["index_number"],
            "kind": row["kind"],
            "defaultValue": row["default_value"],
            "source": row["source"],
            "statements": content.get("statements", []),
        })

    output = {"dimensions": dims}

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    click.echo(f"Exported {len(dims)} dimensions to {output_path}")


@dimensions.command("resolve")
@click.argument("tags_json", type=str)
def dimensions_resolve(tags_json: str):
    """Resolve a set of tags through all loaded dimensions.

    TAGS_JSON is a JSON string of tag key-value pairs, e.g.:
    '{"Environment": "production", "Team": "platform"}'
    """
    _ensure_app_context()
    from app.services.mapping_engine import mapping_engine

    try:
        tags = json.loads(tags_json)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON: {e}", err=True)
        sys.exit(1)

    if not isinstance(tags, dict):
        click.echo("Error: Expected a JSON object of tag key-value pairs.", err=True)
        sys.exit(1)

    mapping_engine.load_dimensions()

    if not mapping_engine.dimensions:
        click.echo("No dimensions loaded. Import dimensions first.")
        return

    results = mapping_engine.resolve_tags(tags)

    click.echo(f"\nInput tags:")
    for k, v in tags.items():
        click.echo(f"  {k}: {v}")

    click.echo(f"\nResolved dimensions:")
    for dim_name, value in results.items():
        marker = "" if value == "Unallocated" else " *"
        click.echo(f"  {dim_name}: {value}{marker}")

    matched = sum(1 for v in results.values() if v != "Unallocated")
    click.echo(f"\n{matched}/{len(results)} dimensions matched.")


# ---------------------------------------------------------------------------
# Sync Commands
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--account-key", required=True, help="Umbrella account key.")
@click.option("--start-date", required=True, help="Start date (YYYY-MM-DD).")
@click.option("--end-date", required=True, help="End date (YYYY-MM-DD).")
@click.option("--vtag-filter", multiple=True, help="Filter to specific dimension names.")
def sync(account_key: str, start_date: str, end_date: str, vtag_filter: tuple):
    """Run a VTagger sync for a date range.

    Fetches assets from Umbrella, applies dimension mappings,
    and generates tagged output files.
    """
    _ensure_app_context()
    from app.services.mapping_engine import mapping_engine
    from app.services.umbrella_client import umbrella_client
    from app.services.sync_service import sync_service

    click.echo("VTagger - Virtual Tagging Agent")
    click.echo("=" * 40)

    # Load dimensions
    mapping_engine.load_dimensions()
    if not mapping_engine.dimensions:
        click.echo("Error: No dimensions loaded. Import dimensions first.", err=True)
        sys.exit(1)
    click.echo(f"Dimensions loaded: {len(mapping_engine.dimensions)}")

    # Authenticate via credential manager
    click.echo("Authenticating...")
    try:
        umbrella_client._ensure_authenticated()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    click.echo("Authenticated successfully.")

    # Run sync
    vtag_filter_dims = list(vtag_filter) if vtag_filter else None

    click.echo(f"\nSyncing: {start_date} to {end_date}")
    if vtag_filter_dims:
        click.echo(f"Filtering dimensions: {vtag_filter_dims}")

    def progress_cb(progress):
        pct = progress.to_dict().get("progress_pct", 0)
        phase = progress.phase
        click.echo(f"  [{pct:.1f}%] {phase} - {progress.processed_assets} assets processed", nl=False)
        click.echo("\r", nl=False)

    try:
        result = sync_service.run_range_sync(
            umbrella_client=umbrella_client,
            mapping_engine=mapping_engine,
            account_key=account_key,
            start_date=start_date,
            end_date=end_date,
            progress_callback=progress_cb,
            vtag_filter_dimensions=vtag_filter_dims,
        )

        click.echo()
        click.echo(f"\nSync {result['status']}:")
        stats = result.get("stats", {})
        click.echo(f"  Total assets:       {stats.get('total_assets', 0)}")
        click.echo(f"  Matched:            {stats.get('matched_assets', 0)}")
        click.echo(f"  Unmatched:          {stats.get('unmatched_assets', 0)}")
        click.echo(f"  Dimension matches:  {stats.get('dimension_matches', 0)}")

        if result.get("output_files"):
            click.echo(f"\nOutput files:")
            for f in result["output_files"]:
                click.echo(f"  {f}")

    except Exception as e:
        click.echo(f"\nError: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Credentials Commands
# ---------------------------------------------------------------------------


@cli.group()
def credentials():
    """Manage VTagger API credentials."""
    pass


@credentials.command("set")
@click.option("--username", prompt="Umbrella username", help="Umbrella API username.")
@click.option(
    "--password",
    prompt="Umbrella password",
    hide_input=True,
    help="Umbrella API password.",
)
def credentials_set(username: str, password: str):
    """Store VTagger API credentials securely."""
    from app.services.credential_manager import set_credentials

    if set_credentials(username, password):
        click.echo("VTagger credentials stored successfully.")
    else:
        click.echo("Error: Failed to store credentials.", err=True)
        sys.exit(1)


@credentials.command("verify")
def credentials_verify():
    """Verify that VTagger credentials are configured and accessible."""
    from app.services.credential_manager import verify_credentials

    success, message = verify_credentials()
    click.echo(message)
    if not success:
        sys.exit(1)


@credentials.command("delete")
@click.confirmation_option(prompt="Are you sure you want to delete VTagger credentials?")
def credentials_delete():
    """Delete stored VTagger credentials."""
    from app.services.credential_manager import delete_credentials

    if delete_credentials():
        click.echo("VTagger credentials deleted.")
    else:
        click.echo("No credentials found to delete.")


@credentials.command("status")
def credentials_status():
    """Check VTagger credentials status."""
    from app.services.credential_manager import has_credentials

    if has_credentials():
        click.echo("VTagger credentials: configured")
    else:
        click.echo("VTagger credentials: not configured")
        click.echo("Run 'vtagger credentials set' to configure.")


# ---------------------------------------------------------------------------
# Server Command
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--host", default="0.0.0.0", help="Host to bind to.")
@click.option("--port", default=8000, type=int, help="Port to bind to.")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development.")
def serve(host: str, port: int, reload: bool):
    """Start the VTagger API server."""
    try:
        import uvicorn
    except ImportError:
        click.echo("Error: uvicorn is required. Install with: pip install uvicorn", err=True)
        sys.exit(1)

    click.echo(f"Starting VTagger API server at http://{host}:{port}")
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload,
    )


# ---------------------------------------------------------------------------
# Info Command
# ---------------------------------------------------------------------------


@cli.command()
def info():
    """Show VTagger configuration and status."""
    _ensure_app_context()
    from app.config import settings
    from app.services.mapping_engine import mapping_engine
    from app.services.credential_manager import has_credentials

    click.echo("VTagger - Virtual Tagging Agent")
    click.echo("=" * 40)
    click.echo(f"Version:        1.0.0")
    click.echo(f"Database:       {settings.database_path}")
    click.echo(f"Output dir:     {settings.output_dir}")
    click.echo(f"API base URL:   {settings.umbrella_api_base}")
    click.echo(f"Batch size:     {settings.batch_size}")

    mapping_engine.load_dimensions()
    click.echo(f"Dimensions:     {len(mapping_engine.dimensions)}")
    click.echo(f"Tag keys:       {len(mapping_engine.get_required_tag_keys())}")

    click.echo(f"Credentials:    {'configured' if has_credentials() else 'not set'}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ensure_app_context():
    """Ensure the application context is initialized (DB, config, etc.)."""
    # Add the backend directory to sys.path if needed
    backend_dir = Path(__file__).resolve().parent.parent
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))

    from app.database import init_database
    init_database()


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------


def main():
    """Entry point for the vtagger CLI."""
    # Ensure backend is on path
    backend_dir = Path(__file__).resolve().parent.parent
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))

    cli()


if __name__ == "__main__":
    main()

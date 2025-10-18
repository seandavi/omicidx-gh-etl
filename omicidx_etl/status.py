"""
Pipeline status dashboard.

Displays status and metrics from pipeline runs, including success rates,
task durations, and recent run history.
"""

from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta
import json
import click
from loguru import logger

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    logger.warning("rich library not available - install with: pip install rich")

from .config import settings


def get_metrics_dir() -> Path:
    """Get the metrics directory path."""
    return Path(settings.PUBLISH_DIRECTORY) / "metrics"


def get_recent_runs(metrics_dir: Path, days: int = 7) -> list[dict]:
    """
    Get pipeline runs from the last N days.

    Args:
        metrics_dir: Directory containing run metrics JSON files
        days: Number of days to look back

    Returns:
        List of run dictionaries, sorted by start time (newest first)
    """
    if not metrics_dir.exists():
        logger.warning(f"Metrics directory not found: {metrics_dir}")
        return []

    cutoff = datetime.now() - timedelta(days=days)
    runs = []

    for metrics_file in metrics_dir.glob("run_*.json"):
        try:
            with open(metrics_file) as f:
                data = json.load(f)

                # Parse run time
                if "start_time" in data:
                    run_time = datetime.fromisoformat(data["start_time"])
                    if run_time > cutoff:
                        runs.append(data)
                else:
                    # Fallback to file name if no start_time
                    # Assumes format: run_YYYYMMDD_HHMMSS.json
                    try:
                        date_str = metrics_file.stem.replace("run_", "")
                        run_time = datetime.strptime(date_str, "%Y%m%d_%H%M%S")
                        if run_time > cutoff:
                            data["start_time"] = run_time.isoformat()
                            runs.append(data)
                    except:
                        # Can't parse time, skip
                        pass

        except Exception as e:
            logger.debug(f"Skipping invalid metrics file {metrics_file}: {e}")
            continue

    # Sort by start time, newest first
    runs.sort(key=lambda x: x.get("start_time", ""), reverse=True)

    return runs


def display_status_rich(runs: list[dict], days: int):
    """Display status using rich terminal UI."""
    console = Console()

    if not runs:
        console.print("[yellow]No pipeline runs found in the last {days} days[/yellow]")
        return

    # Calculate summary stats
    total_runs = len(runs)
    successful_runs = sum(
        1 for run in runs
        if all(
            t.get("status") == "success"
            for t in run.get("tasks", {}).values()
        )
    )

    # Summary panel
    console.print(Panel(
        f"[bold]Total Runs:[/bold] {total_runs}\n"
        f"[bold green]Successful:[/bold green] {successful_runs}\n"
        f"[bold red]Failed:[/bold red] {total_runs - successful_runs}\n"
        f"[bold]Success Rate:[/bold] {successful_runs/total_runs*100:.1f}%",
        title=f"Pipeline Status (Last {days} Days)",
        border_style="blue"
    ))

    # Recent runs table
    table = Table(title=f"\nRecent Runs")
    table.add_column("Run ID", style="cyan", no_wrap=True)
    table.add_column("Start Time", style="dim")
    table.add_column("Status", justify="center")
    table.add_column("Duration", justify="right")
    table.add_column("Tasks", justify="center")

    for run in runs[:15]:  # Show last 15 runs
        run_id = run.get("run_id", "unknown")
        start_time_str = run.get("start_time", "")

        try:
            start_time = datetime.fromisoformat(start_time_str).strftime('%Y-%m-%d %H:%M')
        except:
            start_time = start_time_str[:16] if start_time_str else "unknown"

        tasks = run.get("tasks", {})
        all_success = all(t.get("status") == "success" for t in tasks.values())
        status_icon = "✓" if all_success else "✗"
        status_color = "green" if all_success else "red"

        total_duration = sum(t.get("duration", 0) for t in tasks.values())
        if total_duration >= 3600:
            duration_str = f"{total_duration/3600:.1f}h"
        elif total_duration >= 60:
            duration_str = f"{total_duration/60:.1f}m"
        else:
            duration_str = f"{total_duration:.0f}s"

        successful_tasks = sum(1 for t in tasks.values() if t.get("status") == "success")
        task_summary = f"{successful_tasks}/{len(tasks)}"

        table.add_row(
            run_id,
            start_time,
            f"[{status_color}]{status_icon}[/{status_color}]",
            duration_str,
            task_summary
        )

    console.print(table)

    # Latest run details
    if runs:
        latest = runs[0]
        console.print(f"\n[bold cyan]Latest Run Details:[/bold cyan] {latest.get('run_id', 'unknown')}")

        task_table = Table(show_header=True, header_style="bold")
        task_table.add_column("Task", style="cyan")
        task_table.add_column("Status", justify="center")
        task_table.add_column("Duration", justify="right")

        for task_name, task_data in latest.get("tasks", {}).items():
            status = task_data.get("status", "unknown")
            status_color = "green" if status == "success" else "red"
            duration = task_data.get("duration", 0)

            if duration >= 60:
                duration_str = f"{duration/60:.1f}m"
            else:
                duration_str = f"{duration:.0f}s"

            task_table.add_row(
                task_name,
                f"[{status_color}]{status}[/{status_color}]",
                duration_str
            )

        console.print(task_table)


def display_status_plain(runs: list[dict], days: int):
    """Display status using plain text (fallback when rich not available)."""
    if not runs:
        print(f"No pipeline runs found in the last {days} days")
        return

    # Summary
    total_runs = len(runs)
    successful_runs = sum(
        1 for run in runs
        if all(
            t.get("status") == "success"
            for t in run.get("tasks", {}).values()
        )
    )

    print("=" * 60)
    print(f"Pipeline Status (Last {days} Days)")
    print("=" * 60)
    print(f"Total Runs: {total_runs}")
    print(f"Successful: {successful_runs}")
    print(f"Failed: {total_runs - successful_runs}")
    print(f"Success Rate: {successful_runs/total_runs*100:.1f}%")
    print("=" * 60)

    # Recent runs
    print(f"\nRecent Runs (Last 15):")
    print("-" * 80)
    print(f"{'Run ID':<20} {'Start Time':<17} {'Status':^8} {'Duration':>10} {'Tasks':^8}")
    print("-" * 80)

    for run in runs[:15]:
        run_id = run.get("run_id", "unknown")[:20]
        start_time_str = run.get("start_time", "")

        try:
            start_time = datetime.fromisoformat(start_time_str).strftime('%Y-%m-%d %H:%M')
        except:
            start_time = start_time_str[:16] if start_time_str else "unknown"

        tasks = run.get("tasks", {})
        all_success = all(t.get("status") == "success" for t in tasks.values())
        status_icon = "✓" if all_success else "✗"

        total_duration = sum(t.get("duration", 0) for t in tasks.values())
        if total_duration >= 3600:
            duration_str = f"{total_duration/3600:.1f}h"
        elif total_duration >= 60:
            duration_str = f"{total_duration/60:.1f}m"
        else:
            duration_str = f"{total_duration:.0f}s"

        successful_tasks = sum(1 for t in tasks.values() if t.get("status") == "success")
        task_summary = f"{successful_tasks}/{len(tasks)}"

        print(f"{run_id:<20} {start_time:<17} {status_icon:^8} {duration_str:>10} {task_summary:^8}")

    # Latest run details
    if runs:
        latest = runs[0]
        print(f"\n{'=' * 80}")
        print(f"Latest Run Details: {latest.get('run_id', 'unknown')}")
        print("=" * 80)
        print(f"{'Task':<30} {'Status':^15} {'Duration':>15}")
        print("-" * 80)

        for task_name, task_data in latest.get("tasks", {}).items():
            status = task_data.get("status", "unknown")
            duration = task_data.get("duration", 0)

            if duration >= 60:
                duration_str = f"{duration/60:.1f}m"
            else:
                duration_str = f"{duration:.0f}s"

            print(f"{task_name:<30} {status:^15} {duration_str:>15}")

        print("=" * 80)


# CLI Interface

@click.command()
@click.option(
    '--days',
    default=7,
    help='Show runs from last N days (default: 7)'
)
@click.option(
    '--metrics-dir',
    type=click.Path(path_type=Path),
    default=None,
    help='Directory containing metrics files (default: {PUBLISH_DIRECTORY}/metrics)'
)
@click.option(
    '--plain',
    is_flag=True,
    help='Use plain text output instead of rich formatting'
)
def status(days: int, metrics_dir: Optional[Path], plain: bool):
    """
    Show pipeline status and recent run history.

    Displays a dashboard with:
    - Overall success rate
    - Recent runs with status
    - Task-level details for latest run

    Examples:

        # Show last 7 days
        oidx status

        # Show last 30 days
        oidx status --days 30

        # Use plain text output
        oidx status --plain

        # Custom metrics directory
        oidx status --metrics-dir /custom/path/metrics
    """
    if metrics_dir is None:
        metrics_dir = get_metrics_dir()

    if not metrics_dir.exists():
        logger.error(f"Metrics directory not found: {metrics_dir}")
        logger.info("Run the pipeline at least once to generate metrics")
        raise click.Abort()

    # Get recent runs
    runs = get_recent_runs(metrics_dir, days)

    # Display status
    if plain or not RICH_AVAILABLE:
        if not RICH_AVAILABLE and not plain:
            logger.warning("Using plain text output (install 'rich' for better formatting)")
        display_status_plain(runs, days)
    else:
        display_status_rich(runs, days)


if __name__ == "__main__":
    status()

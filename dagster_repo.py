# dagster_repo.py
import os
import subprocess
from pathlib import Path
from dagster import Definitions, op, job, ScheduleDefinition

AZ_VAR = "AZURE_UBERRIDES_STORAGE_CONNECTION_STRING" 
AZ_CONTAINER = "raw"               
GEN_DAYS_WEEKLY = "7"
SEED = "17"
MEAN = "1000"
PYTHON = "python"                            # or "python3" if needed

PROJECT_ROOT = Path(__file__).resolve().parent
GEN_SCRIPT = PROJECT_ROOT / "generate_rides.py"
STATE_FILE = PROJECT_ROOT / ".uberides.env"

def _run_generator(days: str):
    if not GEN_SCRIPT.exists():
        raise FileNotFoundError(f"generate_rides.py not found at {GEN_SCRIPT}")

    # Ensure subprocess inherits Azure connection string
    env = os.environ.copy()
    conn = env.get(AZ_VAR)
    if not conn:
        raise EnvironmentError(
            f"{AZ_VAR} is not set. Export your Azure connection string before running Dagster."
        )
    env["AZURE_STORAGE_CONNECTION_STRING"] = conn
    cmd = [
        PYTHON, str(GEN_SCRIPT),
        "--out", "azure",
        "--azure-container", AZ_CONTAINER,
        "--days", days,
        "--seed", SEED,
        "--mean", MEAN,
        "--state-file", str(STATE_FILE),
    ]
    # stream logs to Dagster
    subprocess.run(cmd, check=True, env=env)

@op
def generate_weekly_partition():
    _run_generator(GEN_DAYS_WEEKLY)

@job
def uberides_weekly_job():
    generate_weekly_partition()

weekly_schedule = ScheduleDefinition(
    name="uberides_weekly_schedule",
    job=uberides_weekly_job,
    cron_schedule="44 19 * * 6",  # Fri 7:45 PM
    execution_timezone="America/Phoenix",
)

defs = Definitions(
    jobs=[uberides_weekly_job],
    schedules=[weekly_schedule]
)
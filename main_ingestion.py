"""Ingestion Plane Official Entrypoint.

- Single execution root for ingestion.
- Scheduling, budget, and snapshot execution are delegated.
- Serving Plane is intentionally excluded.
"""

from scripts.step7_run_orchestrator_once import main as run_ingestion_once

if __name__ == "__main__":
    run_ingestion_once()


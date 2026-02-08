"""
VTagger Simulation Service.

Runs simulation/dry-run tagging to preview results without uploading.
Provides statistics and sample records for review.

Ported from BPVtagger with generic dimension handling.
"""

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.config import settings
from app.services.agent_logger import log_timing
from app.services.tagging_engine import TaggingEngine, MappingStats


@dataclass
class SimulationResults:
    """Results of a simulation run."""

    status: str = "pending"
    start_date: str = ""
    end_date: str = ""
    total_assets: int = 0
    matched_assets: int = 0
    unmatched_assets: int = 0
    dimension_matches: int = 0
    match_rate: float = 0.0
    vtag_names: List[str] = field(default_factory=list)
    dimension_details: Dict[str, int] = field(default_factory=dict)
    sample_records: List[Dict] = field(default_factory=list)
    elapsed_seconds: float = 0.0
    error_message: str = ""
    output_file: str = ""

    def to_dict(self) -> dict:
        """Convert results to dictionary."""
        return {
            "status": self.status,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_assets": self.total_assets,
            "matched_assets": self.matched_assets,
            "unmatched_assets": self.unmatched_assets,
            "dimension_matches": self.dimension_matches,
            "match_rate": self.match_rate,
            "vtag_names": self.vtag_names,
            "dimension_details": self.dimension_details,
            "sample_records": self.sample_records[:50],
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "error_message": self.error_message,
            "output_file": self.output_file,
        }


class SimulationService:
    """Runs simulation/dry-run tagging operations."""

    def __init__(self):
        self._engine: Optional[TaggingEngine] = None
        self._results: Optional[SimulationResults] = None

    @property
    def engine(self) -> Optional[TaggingEngine]:
        """Get the current tagging engine instance."""
        return self._engine

    def cancel(self):
        """Cancel the current simulation."""
        if self._engine:
            self._engine.cancel()

    def get_progress(self) -> dict:
        """Get current simulation progress."""
        if self._engine:
            return self._engine.get_progress()
        return {"status": "idle"}

    def get_results(self) -> Optional[dict]:
        """Get the results of the last simulation."""
        if self._results:
            return self._results.to_dict()
        return None

    def run_simulation(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str,
        start_date: str,
        end_date: str,
        progress_callback: Optional[Callable] = None,
        vtag_filter_dimensions: Optional[List[str]] = None,
    ) -> SimulationResults:
        """
        Run a simulation/dry-run tagging.

        Args:
            umbrella_client: Client for Umbrella API.
            mapping_engine: Mapping engine with loaded dimensions.
            account_key: Umbrella account key.
            start_date: Start date (YYYY-MM-DD).
            end_date: End date (YYYY-MM-DD).
            progress_callback: Optional progress callback.
            vtag_filter_dimensions: Optional list of dimensions to filter.

        Returns:
            SimulationResults with statistics and sample data.
        """
        log_timing(f"Starting simulation: {start_date} to {end_date}, account={account_key}")

        results = SimulationResults(
            status="running",
            start_date=start_date,
            end_date=end_date,
        )
        self._results = results

        # Create a tagging engine for this simulation
        engine = TaggingEngine()
        self._engine = engine

        start_time = time.time()

        try:
            output_file, stats = engine.fetch_and_map(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=account_key,
                start_date=start_date,
                end_date=end_date,
                output_dir=settings.output_dir,
                progress_callback=progress_callback,
                vtag_filter_dimensions=vtag_filter_dimensions,
            )

            elapsed = time.time() - start_time

            # Populate results from stats
            stats_dict = stats.to_dict()
            results.status = "completed" if not engine.is_cancelled() else "cancelled"
            results.total_assets = stats_dict["total_assets"]
            results.matched_assets = stats_dict["matched_assets"]
            results.unmatched_assets = stats_dict["unmatched_assets"]
            results.dimension_matches = stats_dict["dimension_matches"]
            results.match_rate = stats_dict["match_rate"]
            results.dimension_details = stats_dict["dimension_details"]
            results.sample_records = engine.progress.sample_records
            results.elapsed_seconds = elapsed
            results.output_file = output_file

            # Get vtag names from mapping engine's sorted dimensions
            results.vtag_names = [
                d.vtag_name for d in mapping_engine._sorted_dimensions
            ]
            if vtag_filter_dimensions:
                results.vtag_names = [
                    d for d in results.vtag_names if d in vtag_filter_dimensions
                ]

            log_timing(
                f"Simulation complete: {results.total_assets} assets, "
                f"{results.matched_assets} matched ({results.match_rate}%), "
                f"{results.dimension_matches} dimension matches"
            )

            return results

        except Exception as e:
            elapsed = time.time() - start_time
            results.status = "error"
            results.error_message = str(e)
            results.elapsed_seconds = elapsed
            log_timing(f"Simulation error: {e}")
            return results

        finally:
            self._engine = None


# Global instance
simulation_service = SimulationService()

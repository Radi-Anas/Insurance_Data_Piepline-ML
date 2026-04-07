"""
pipeline_state.py
Manages pipeline state for incremental loading.
Tracks last run timestamp and processed URLs.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Set

logger = logging.getLogger(__name__)


class PipelineState:
    """
    Manages pipeline execution state for incremental loading.
    
    Tracks:
    - last_run_timestamp: When pipeline last ran
    - last_successful_run: Last successful run timestamp
    - processed_urls: URLs already processed (for deduplication)
    - row_counts_history: Historical row counts (for trend analysis)
    """
    
    def __init__(self, state_file: str = "logs/pipeline_state.json"):
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state from file or return default."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("State file corrupted, starting fresh")
        
        return {
            "last_run_timestamp": None,
            "last_successful_run": None,
            "last_run_id": None,
            "processed_urls": [],
            "row_counts_history": [],
            "errors_history": [],
        }
    
    def _save_state(self):
        """Save state to file."""
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, indent=2, default=str)
        logger.debug(f"[STATE] Saved to {self.state_file}")
    
    def mark_run_start(self, run_id: str):
        """Mark the start of a pipeline run."""
        self.state["last_run_timestamp"] = datetime.now().isoformat()
        self.state["last_run_id"] = run_id
        logger.info(f"[STATE] Run {run_id} started at {self.state['last_run_timestamp']}")
    
    def mark_run_success(self, metrics: Dict[str, Any]):
        """Mark the run as successful."""
        self.state["last_successful_run"] = datetime.now().isoformat()
        self.state["last_run_id"] = metrics.get("run_id")
        
        # Add to history
        self.state["row_counts_history"].append({
            "timestamp": self.state["last_successful_run"],
            "rows_extracted": metrics.get("rows_extracted", 0),
            "rows_transformed": metrics.get("rows_transformed", 0),
            "rows_loaded": metrics.get("rows_loaded", 0),
            "quality_score": metrics.get("data_quality_score", 0),
        })
        
        # Keep only last 30 runs in history
        if len(self.state["row_counts_history"]) > 30:
            self.state["row_counts_history"] = self.state["row_counts_history"][-30:]
        
        # Add processed URLs
        if "processed_urls" in metrics:
            self.state["processed_urls"] = list(
                set(self.state["processed_urls"]) | set(metrics["processed_urls"])
            )
        
        self._save_state()
        logger.info(f"[STATE] Run successful at {self.state['last_successful_run']}")
    
    def mark_run_failure(self, error: str):
        """Record a failed run."""
        self.state["errors_history"].append({
            "timestamp": datetime.now().isoformat(),
            "error": error,
        })
        
        if len(self.state["errors_history"]) > 30:
            self.state["errors_history"] = self.state["errors_history"][-30:]
        
        self._save_state()
        logger.error(f"[STATE] Run failed: {error}")
    
    def get_last_run(self) -> Optional[Dict[str, Any]]:
        """Get information about the last run."""
        if not self.state["last_successful_run"]:
            return None
        
        return {
            "timestamp": self.state["last_successful_run"],
            "run_id": self.state.get("last_run_id"),
            "time_since_last": self._time_since_last(),
            "total_urls_processed": len(self.state.get("processed_urls", [])),
        }
    
    def _time_since_last(self) -> str:
        """Get human-readable time since last run."""
        if not self.state["last_successful_run"]:
            return "Never"
        
        from datetime import datetime as dt
        last = dt.fromisoformat(self.state["last_successful_run"])
        now = dt.now()
        delta = now - last
        
        if delta.days > 0:
            return f"{delta.days}d ago"
        elif delta.seconds > 3600:
            return f"{delta.seconds // 3600}h ago"
        else:
            return f"{delta.seconds // 60}m ago"
    
    def is_url_processed(self, url: str) -> bool:
        """Check if a URL has already been processed."""
        return url in self.state.get("processed_urls", [])
    
    def get_trend(self, metric: str = "rows_loaded", last_n: int = 7) -> list:
        """Get trend for a metric over last N runs."""
        history = self.state.get("row_counts_history", [])
        if not history:
            return []
        
        values = [h.get(metric, 0) for h in history[-last_n:]]
        return values
    
    def should_rerun(self, min_interval_hours: int = 6) -> bool:
        """Check if enough time has passed to warrant a rerun."""
        if not self.state["last_successful_run"]:
            return True
        
        from datetime import datetime as dt
        last = dt.fromisoformat(self.state["last_successful_run"])
        now = dt.now()
        hours_since = (now - last).seconds / 3600
        
        return hours_since >= min_interval_hours
    
    def reset(self):
        """Reset all state (for testing)."""
        self.state = {
            "last_run_timestamp": None,
            "last_successful_run": None,
            "last_run_id": None,
            "processed_urls": [],
            "row_counts_history": [],
            "errors_history": [],
        }
        self._save_state()
        logger.info("[STATE] Reset to default")


# Global state instance
_current_state: Optional[PipelineState] = None


def get_state() -> PipelineState:
    """Get or create the current state instance."""
    global _current_state
    if _current_state is None:
        _current_state = PipelineState()
    return _current_state
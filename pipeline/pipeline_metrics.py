"""
pipeline_metrics.py
Tracks pipeline execution metrics for monitoring and observability.
"""

import time
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class PipelineMetrics:
    """
    Tracks and logs pipeline execution metrics.
    
    Metrics tracked:
    - rows_extracted: Number of raw rows fetched
    - rows_transformed: Number of rows after cleaning
    - rows_loaded: Number of rows written to DB
    - rows_dropped: Rows removed during transformation
    - extraction_time_seconds: Time to fetch data
    - transformation_time_seconds: Time to clean data
    - load_time_seconds: Time to write to DB
    - total_time_seconds: End-to-end execution time
    - data_quality_score: Percentage of valid data
    - errors: Any errors encountered
    """
    
    def __init__(self, run_id: Optional[str] = None):
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = time.time()
        self.metrics = {
            "run_id": self.run_id,
            "start_time": datetime.now().isoformat(),
            "rows_extracted": 0,
            "rows_transformed": 0,
            "rows_loaded": 0,
            "rows_dropped": 0,
            "extraction_time_seconds": 0,
            "transformation_time_seconds": 0,
            "load_time_seconds": 0,
            "data_quality_score": 0.0,
            "errors": [],
            "warnings": [],
        }
    
    def start_extraction(self):
        """Mark start of extraction phase."""
        self._extraction_start = time.time()
    
    def end_extraction(self, rows: int):
        """Mark end of extraction phase."""
        self.metrics["extraction_time_seconds"] = round(
            time.time() - self._extraction_start, 2
        )
        self.metrics["rows_extracted"] = rows
        logger.info(f"[METRICS] Extracted {rows} rows in {self.metrics['extraction_time_seconds']}s")
    
    def start_transformation(self):
        """Mark start of transformation phase."""
        self._transformation_start = time.time()
    
    def end_transformation(self, rows_output: int, rows_dropped: int):
        """Mark end of transformation phase."""
        self.metrics["transformation_time_seconds"] = round(
            time.time() - self._transformation_start, 2
        )
        self.metrics["rows_transformed"] = rows_output
        self.metrics["rows_dropped"] = rows_dropped
        
        if self.metrics["rows_extracted"] > 0:
            self.metrics["data_quality_score"] = round(
                (rows_output / self.metrics["rows_extracted"]) * 100, 1
            )
        
        logger.info(
            f"[METRICS] Transformed: {rows_output} output, "
            f"{rows_dropped} dropped, "
            f"quality score: {self.metrics['data_quality_score']}%"
        )
    
    def start_load(self):
        """Mark start of load phase."""
        self._load_start = time.time()
    
    def end_load(self, rows: int):
        """Mark end of load phase."""
        self.metrics["load_time_seconds"] = round(
            time.time() - self._load_start, 2
        )
        self.metrics["rows_loaded"] = rows
        logger.info(f"[METRICS] Loaded {rows} rows in {self.metrics['load_time_seconds']}s")
    
    def add_error(self, error: str):
        """Log an error."""
        self.metrics["errors"].append(error)
        logger.error(f"[METRICS] Error: {error}")
    
    def add_warning(self, warning: str):
        """Log a warning."""
        self.metrics["warnings"].append(warning)
        logger.warning(f"[METRICS] Warning: {warning}")
    
    def finalize(self) -> Dict[str, Any]:
        """Finalize and return all metrics."""
        self.metrics["end_time"] = datetime.now().isoformat()
        self.metrics["total_time_seconds"] = round(
            time.time() - self.start_time, 2
        )
        
        logger.info(
            f"[METRICS] Pipeline complete in {self.metrics['total_time_seconds']}s | "
            f"Extracted: {self.metrics['rows_extracted']} | "
            f"Transformed: {self.metrics['rows_transformed']} | "
            f"Loaded: {self.metrics['rows_loaded']} | "
            f"Quality: {self.metrics['data_quality_score']}%"
        )
        
        return self.metrics
    
    def save_to_file(self, output_dir: str = "logs"):
        """Save metrics to JSON file."""
        Path(output_dir).mkdir(exist_ok=True)
        filepath = Path(output_dir) / f"pipeline_{self.run_id}.json"
        
        with open(filepath, "w") as f:
            json.dump(self.metrics, f, indent=2)
        
        logger.info(f"[METRICS] Saved to {filepath}")
        return filepath
    
    def get_summary(self) -> str:
        """Get a human-readable summary."""
        return (
            f"Pipeline run {self.run_id} | "
            f"Time: {self.metrics['total_time_seconds']}s | "
            f"Extracted: {self.metrics['rows_extracted']} | "
            f"Transformed: {self.metrics['rows_transformed']} | "
            f"Loaded: {self.metrics['rows_loaded']} | "
            f"Quality: {self.metrics['data_quality_score']}%"
        )


# Global metrics instance
_current_metrics: Optional[PipelineMetrics] = None


def get_metrics() -> PipelineMetrics:
    """Get or create the current metrics instance."""
    global _current_metrics
    if _current_metrics is None:
        _current_metrics = PipelineMetrics()
    return _current_metrics


def reset_metrics():
    """Reset the metrics instance."""
    global _current_metrics
    _current_metrics = None

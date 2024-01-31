from dataclasses import dataclass
from typing import List, Optional

from pluggy import HookspecMarker

hookspec = HookspecMarker("ray")


@dataclass
class Dataset:
    path: str


@dataclass
class Checkpoint:
    path: str


@dataclass
class Lineage:
    run_id: str
    name: str
    input_checkpoints: Optional[List[Checkpoint]] = None
    output_checkpoints: Optional[List[Checkpoint]] = None
    input_datasets: Optional[List[Dataset]] = None
    output_datasets: Optional[List[Dataset]] = None


@hookspec
def on_run_start(lineage: Lineage):
    """Execute when run state changes to RUNNING."""


@hookspec
def on_run_complete(lineage: Lineage):
    """Execute when run state changes to SUCCESS."""


@hookspec
def on_run_fail(lineage: Lineage):
    """Execute when run state changes to FAIL."""

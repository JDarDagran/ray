import threading
import time
from enum import Enum
from uuid import uuid4

import ray
import logging
from ray.data.context import DataContext
from ray.lineage.listener import get_listener_manager
from ray.lineage.spec import Checkpoint, Dataset, Lineage
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

LINEAGE_ACTOR_NAME = "lineage_actor"
LINEAGE_ACTOR_NAMESPACE = "_lineage_actor"


class RunState(Enum):
    STARTED = "STARTED"
    FAILED = "FAILED"
    COMPLETE = "COMPLETE"


@ray.remote(num_cpus=0)
class _LineageActor:
    """TODO: Doc"""

    def __init__(self, max_stats=1000):
        self.runs = {}

        self.datasets = {}
        self.input_checkpoints = set()
        self.output_checkpoints = set()

    @property
    def started_runs(self):
        # TODO: detect it better what's current run for the dataset?
        # can there be 2+ runs running in single Ray cluster?
        return [
            run_id
            for run_id, keys in self.runs.items()
            if keys["state"] == RunState.STARTED
        ]

    def create_dataset_lineage(self, dataset_uuid, parents, job_uuid, paths):
        self.datasets[dataset_uuid] = {"parents": parents, "paths": paths}
        if job_uuid in self.runs:
            self.runs[job_uuid].setdefault("datasets", set()).update(
                self.resolve_dataset_lineage(dataset_uuid)
            )

    def register_input_checkpoint(self, checkpoint_path, job_uuid):
        self.input_checkpoints.add(checkpoint_path)
        if job_uuid in self.runs:
            self.runs[job_uuid].setdefault("input_checkpoints", set()).add(
                checkpoint_path
            )

    def register_output_checkpoint(self, checkpoint_path, job_uuid):
        self.output_checkpoints.add(checkpoint_path)
        if job_uuid in self.runs:
            self.runs[job_uuid].setdefault("output_checkpoints", set()).add(
                checkpoint_path
            )

    def update_dataset_paths(self, dataset_uuid, paths):
        self.datasets[dataset_uuid]["paths"] = paths

    def resolve_dataset_lineage(self, dataset_uuid):
        def find_deepest_parent(node, graph):
            # probably needs rewriting
            parents = graph[node]["parents"]
            if parents is None:
                return set(graph[node]["paths"])
            else:
                return find_deepest_parent(parents[0], graph)

        return find_deepest_parent(dataset_uuid, self.datasets)

    def get_datasets_registered(self):
        return self.datasets

    def register_run(self, run_id, job_uuid, name, params):
        self.runs[job_uuid] = {
            "state": RunState.STARTED,
            "start_time": time.time(),
            "run_id": run_id,
            "name": name,
            **params,
        }
        get_listener_manager().hook.on_run_start(
            lineage=Lineage(run_id=run_id, name=name)
        )

    def complete_run(self, run_id, job_uuid):
        self.runs[job_uuid]["state"] = RunState.COMPLETE
        self.runs[job_uuid]["update_time"] = time.time()
        get_listener_manager().hook.on_run_complete(
            lineage=Lineage(
                run_id=run_id,
                name=self.runs[job_uuid]["name"],
                input_checkpoints=[
                    Checkpoint(path)
                    for path in self.runs[job_uuid].get("input_checkpoints", [])
                ],
                output_checkpoints=[
                    Checkpoint(path)
                    for path in self.runs[job_uuid].get("output_checkpoints", [])
                ],
                input_datasets=[
                    Dataset(path) for path in self.runs[job_uuid].get("datasets", [])
                ],
            )
        )

    def get_runs(self):
        return self.runs


# Creating/getting an actor from multiple threads is not safe.
# https://github.com/ray-project/ray/issues/41324
_lineage_actor_lock: threading.RLock = threading.RLock()


def _get_or_create_lineage_actor():
    ctx = DataContext.get_current()
    scheduling_strategy = ctx.scheduling_strategy
    if not ray.util.client.ray.is_connected():
        # Pin the stats actor to the local node
        # so it fate-shares with the driver.
        scheduling_strategy = NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(),
            soft=False,
        )
    with _lineage_actor_lock:
        return _LineageActor.options(
            name=LINEAGE_ACTOR_NAME,
            namespace=LINEAGE_ACTOR_NAMESPACE,
            get_if_exists=True,
            lifetime="detached",
            scheduling_strategy=scheduling_strategy,
        ).remote()


class _LineageManager:
    """TODO: Document class."""

    def __init__(self):
        # Lazily get stats actor handle to avoid circular import.
        self._lineage_actor_handle = None
        self._lineage_actor_cluster_id = None

    def _lineage_actor(self, create_if_not_exists=False) -> _LineageActor:
        if ray._private.worker._global_node is None:
            raise RuntimeError("Global node is not initialized.")
        current_cluster_id = ray._private.worker._global_node.cluster_id
        if (
            self._lineage_actor_handle is None
            or self._lineage_actor_cluster_id != current_cluster_id
        ):
            try:
                self._lineage_actor_handle = ray.get_actor(
                    name=LINEAGE_ACTOR_NAME, namespace=LINEAGE_ACTOR_NAMESPACE
                )
            except ValueError:
                self._lineage_actor_handle = _get_or_create_lineage_actor()
            self._lineage_actor_cluster_id = current_cluster_id
        return self._lineage_actor_handle

    def register_dataset_lineage(self, dataset_uuid, parents, job_uuid, paths=None):
        self._lineage_actor().create_dataset_lineage.remote(
            dataset_uuid, parents, job_uuid, paths
        )

    def register_input_checkpoint(self, checkpoint_path, job_uuid):
        self._lineage_actor().register_input_checkpoint.remote(
            checkpoint_path, job_uuid
        )

    def register_output_checkpoint(self, checkpoint_path, job_uuid):
        self._lineage_actor().register_output_checkpoint.remote(
            checkpoint_path, job_uuid
        )

    def update_dataset_paths(self, dataset_uuid, paths):
        self._lineage_actor().update_dataset_paths.remote(dataset_uuid, paths)

    def get_datasets_registered(self):
        return ray.get(self._lineage_actor().get_datasets_registered.remote())

    def register_run(self, name, job_uuid, params=None):
        run_id = uuid4()
        params = params or {}
        self._lineage_actor().register_run.remote(run_id, job_uuid, name, params=params)
        return run_id

    def complete_run(self, run_id, job_uuid):
        self._lineage_actor().complete_run.remote(run_id, job_uuid)

    def get_runs(self):
        return ray.get(self._lineage_actor().get_runs.remote())


LineageManager = _LineageManager()

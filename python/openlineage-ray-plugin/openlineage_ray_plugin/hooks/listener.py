import pluggy
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
import datetime

hookimpl = pluggy.HookimplMarker("ray")

# TODO: Add more real-world example of OL client emit.
# This needs more work on naming convention for jobs and checkpoints.


@hookimpl
def on_run_start(lineage):
    OpenLineageClient(url="http://localhost:5000").emit(
        RunEvent(
            eventTime=datetime.datetime.now().isoformat(),
            eventType=RunState.START,
            run=Run(runId=str(lineage.run_id)),
            job=Job(name=lineage.name, namespace="test-namespace", facets={}),
            inputs=[
                Dataset(name=dataset.path, namespace="test-namespace", facets={})
                for dataset in lineage.input_datasets or []
            ],
            outputs=[
                Dataset(name=checkpoint, namespace="test-namespace", facets={})
                for checkpoint in lineage.output_checkpoints or []
            ],
            producer="http://ray",
        )
    )


@hookimpl
def on_run_complete(lineage):
    OpenLineageClient(url="http://localhost:5000").emit(
        RunEvent(
            eventTime=datetime.datetime.now().isoformat(),
            eventType=RunState.COMPLETE,
            run=Run(runId=str(lineage.run_id)),
            job=Job(name=lineage.name, namespace="test-namespace", facets={}),
            inputs=[
                Dataset(name=dataset.path, namespace="test-namespace")
                for dataset in lineage.input_datasets or []
            ] + 
            [
                Dataset(name=checkpoint.path, namespace="test-namespace")
                for checkpoint in lineage.input_checkpoints or []
            ],
            outputs=[
                Dataset(name=checkpoint.path, namespace="test-namespace")
                for checkpoint in lineage.output_checkpoints or []
            ],
            producer="http://ray",
        )
    )

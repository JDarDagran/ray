import pluggy

hookimpl = pluggy.HookimplMarker("ray")

# TODO: Add more real-world example of OL client emit.
# This needs more work on naming convention for jobs and checkpoints.


@hookimpl
def on_run_start(lineage):
    print("START", lineage)


@hookimpl
def on_run_complete(lineage):
    print("COMPLETE", lineage)

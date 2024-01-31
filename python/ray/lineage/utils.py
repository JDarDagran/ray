from functools import wraps


def wrap_datasource_reader(fn):
    @wraps(fn)
    def reader_wrapper(*args, **kwargs):
        from ray.data.datasource.file_based_datasource import FileBasedDatasource
        from ray.lineage.actor import LineageManager

        datasource = kwargs.get("datasource", None) or args[0]

        dataset = fn(*args, **kwargs)
        if datasource:
            if isinstance(datasource, FileBasedDatasource):
                fs = datasource._filesystem.type_name
                LineageManager.update_dataset_paths(
                    dataset._uuid, [f"{fs}://{path}" for path in datasource._paths()]
                )
        return dataset

    return reader_wrapper

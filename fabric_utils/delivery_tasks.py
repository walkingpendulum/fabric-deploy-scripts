# coding: utf-8

import os
from collections import namedtuple

import yaml

from fabric_utils import artifactory
from fabric_utils.models import build_worker_to_registry_mapping
from fabric_utils.paths import DATA_PATH, ARTIFACTORY_MODEL_TAGS_TABLE_PATH
from fabric_utils.svc import update_tags_table

_LocalPath = namedtuple('LocalPath', ['folder', 'file'])


# noinspection PyPep8Naming
def LocalPath(folder, file_=None):
    return _LocalPath(folder, file_)


def _collect_tasks_for_models_loading():
    worker_to_registry_mapping = build_worker_to_registry_mapping()
    update_tags_table(worker_to_registry_mapping)

    with open(ARTIFACTORY_MODEL_TAGS_TABLE_PATH) as f:
        tags_table = yaml.load(f.read())

    tasks = []
    for worker, model_name_to_tag_mapping in tags_table.items():
        for model_name, tag in model_name_to_tag_mapping.items():
            cls = worker_to_registry_mapping[worker][model_name]
            url = '{prefix}/models/{worker}/{model}-{tag}.tar.gz'.format(
                prefix=artifactory.ARTIFACTORY_PREFIX,
                worker=worker,
                model=cls.model_src_name,
                tag=tag,
            )
            dst_folder = os.path.join(DATA_PATH, worker, 'models', cls.model_src_name)

            local_path_obj = LocalPath(dst_folder)
            tasks.append((url, local_path_obj))

    return tasks


def collect_tasks():
    tasks = []

    # vertica
    url = '%s/libs/vertica/libverticaodbc.so.7.1.1' % artifactory.ARTIFACTORY_PREFIX
    file_name = 'libverticaodbc.so.7.1.1'
    local_path_obj = LocalPath(DATA_PATH, file_name)
    tasks.append((url, local_path_obj))

    _tasks = [
        # wordforms
        (
            '%s/common/wordforms-latest.tar.gz',
            os.path.join('common', 'wordforms')
        ),

        # features
        (
            '%s/features/Data-latest.tar.gz',
            'Data'
        )
    ]

    tasks.extend(map(
        lambda (template, path): (
            template % artifactory.ARTIFACTORY_PREFIX,
            os.path.join(DATA_PATH, path)
        ),
        _tasks
    ))

    # models
    models_tasks = _collect_tasks_for_models_loading()
    tasks.extend(models_tasks)

    return tasks

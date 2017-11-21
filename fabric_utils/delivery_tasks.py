# coding: utf-8
from __future__ import print_function

import os
import sys

import yaml

from fabric_utils.models import build_worker_to_registry_mapping
from fabric_utils.paths import DATA_PATH, ARTIFACTORY_MODEL_TAGS_TABLE_PATH
from fabric_utils.svc import update_tags_table
from fabric_utils.utils import LocalPath


def _collect_tasks_for_models_loading(worker_name_mask):
    worker_to_registry_mapping = build_worker_to_registry_mapping(worker_name_mask)
    update_tags_table(worker_to_registry_mapping)

    with open(ARTIFACTORY_MODEL_TAGS_TABLE_PATH) as f:
        tags_table = yaml.load(f.read())

    tasks = []
    for worker, model_name_to_tag_mapping in tags_table.items():
        try:
            registry = worker_to_registry_mapping[worker]
        except KeyError:
            # skip not found
            print('skip %s' % worker, file=sys.stderr)
            continue

        for model_name, tag in model_name_to_tag_mapping.items():
            cls = registry[model_name]
            uri = 'models/{worker}/{model}-{tag}.tar.gz'.format(
                worker=worker,
                model=cls.model_src_name,
                tag=tag,
            )
            dst_folder = os.path.join(DATA_PATH, worker, 'models', cls.model_src_name)

            local_path_obj = LocalPath(dst_folder)
            tasks.append((uri, local_path_obj))
    return tasks


def collect_tasks(worker_name_mask):
    tasks = []

    _tasks = [
        # vertica
        (
            'libs/vertica/libverticaodbc-latest.tar.gz',
            'vertica/',
        ),
        # wordforms
        (
            'common/wordforms-latest.tar.gz',
            os.path.join('common', 'wordforms')
        ),

        # features
        (
            'features/Data-latest.tar.gz',
            'Data'
        )
    ]

    tasks.extend(map(
        lambda (uri, path): (uri, LocalPath(os.path.abspath(os.path.join(DATA_PATH, path)))),
        _tasks
    ))

    # models
    models_tasks = _collect_tasks_for_models_loading(worker_name_mask)
    tasks.extend(models_tasks)

    return tasks

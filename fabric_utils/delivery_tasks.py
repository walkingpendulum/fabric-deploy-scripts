# coding: utf-8
from __future__ import print_function

import os
import re

import yaml

from fabric_utils.paths import DATA_PATH, ARTIFACTORY_MODEL_TAGS_TABLE_PATH

ARTIFACTORY_URL_PREFIX = 'http://artifactory/service-local'
ARTIFACTORY_INFO_PREFIX = 'http://artifactory/api/storage/service-local'


def _collect_tasks_for_models_loading(worker_name_mask):
    with open(ARTIFACTORY_MODEL_TAGS_TABLE_PATH) as f:
        tags_table = yaml.load(f.read())

    tasks = []
    for worker, model_name_to_src_tag_mapping in tags_table.items():
        if not re.findall(worker_name_mask, worker):
            continue

        for model_name, src_tag_mapping in model_name_to_src_tag_mapping.items():
            uri = 'models/{worker}/{mapping[src]}-{mapping[tag]}.tar.gz'.format(
                worker=worker,
                mapping=src_tag_mapping
            )
            dst_folder = os.path.join(DATA_PATH, worker, 'models', src_tag_mapping['src'])

            task = {'__uri': uri, 'destination': {'folder': dst_folder}}
            tasks.append(task)

    return tasks


def collect_tasks(worker_name_mask):
    tasks = [
        # vertica
        {
            '__uri': 'libs/vertica/libverticaodbc-latest.tar.gz',
            'destination': {'folder': os.path.abspath(os.path.join(DATA_PATH, 'vertica'))},
        },
        # wordforms
        {
            '__uri': 'common/wordforms-latest.tar.gz',
            'destination': {'folder': os.path.abspath(os.path.join(DATA_PATH, 'common', 'wordforms'))},
        },
        # features
        {
            '__uri': 'features/Data-latest.tar.gz',
            'destination': {'folder': os.path.abspath(os.path.join(DATA_PATH, 'Data'))},
        },
        {
            '__uri': 'common/vin_parsing-20180502.tar.gz',
            'destination': {'folder': os.path.abspath(os.path.join(DATA_PATH, 'common', 'vin_parsing'))},
        },
    ]

    # models
    models_tasks = _collect_tasks_for_models_loading(worker_name_mask)
    tasks += models_tasks

    for task in tasks:
        uri = task.pop('__uri')
        task.update({
            'url': '%s/%s' % (ARTIFACTORY_URL_PREFIX, uri),
            'info_url': '%s/%s' % (ARTIFACTORY_INFO_PREFIX, uri)
        })

    return tasks

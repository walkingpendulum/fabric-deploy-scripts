# coding=utf-8
import glob
import importlib
import os
import sys

from fabric_utils.paths import GIT_ROOT


def build_worker_to_registry_mapping():
    """Обходит все воркеры сервиса и строит маппинг названий модели на классы.

    Используется черная магия с импортами и путями.
    
    :return: 
    """
    sys.path.insert(0, GIT_ROOT)
    packages = glob.glob(os.path.join('service', '*', 'models'))
    worker_to_models_mapping = {}
    for path in packages:
        _, worker_name, _ = path.rsplit('/', 2)
        module = importlib.import_module(path.replace('/', '.'))
        worker_to_models_mapping[worker_name] = module.models

    return worker_to_models_mapping

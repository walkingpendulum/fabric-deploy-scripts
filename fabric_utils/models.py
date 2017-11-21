# coding=utf-8
import glob
import importlib
import os
import sys

from fabric_utils.paths import GIT_ROOT


def build_worker_to_registry_mapping(worker_name_mask='*'):
    """Обходит все воркеры сервиса и строит маппинг названий модели на классы.

    Используется черная магия с импортами и путями.
    :param worker_name_mask: str, маска, по которой будем искать директорию воркера. по умолчанию '*'
    
    :return: 
    """
    sys.path.insert(0, GIT_ROOT)
    packages = glob.glob(os.path.join('service', worker_name_mask, 'models'))
    worker_to_models_mapping = {}
    for path in packages:
        try:
            _, curr_worker_name,  _ = path.rsplit('/', 2)
            module = importlib.import_module(path.replace('/', '.'))
            worker_to_models_mapping[curr_worker_name] = module.models
        except (AttributeError, ImportError):
            pass
    return worker_to_models_mapping

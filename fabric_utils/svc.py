# coding=utf-8
import os
from collections import defaultdict

import yaml
from fabric import api

from fabric_utils.models import build_worker_to_registry_mapping
from fabric_utils.paths import ARTIFACTORY_MODEL_TAGS_TABLE_PATH, GIT_ROOT


class GitTreeHandler(object):
    @staticmethod
    def info():
        with api.cd(GIT_ROOT):
            api.sudo('git fetch')
            lines = api.run('git branch -vv')
            info_str_ = next(line[len('* '):] for line in lines.split('\n') if line.startswith('* '))

        return info_str_

    @staticmethod
    def is_index_empty():
        with api.cd(GIT_ROOT), api.settings(warn_only=True):
            result = api.run("git diff-files --quiet")

        return not result.return_code


def update_tags_table(worker_to_registry_mapping=None):
    """Обновляет/создает таблицу с используемыми тегами для моделей
    
    Таблица тегов нужна для правильного выбора версии файла для каждой модели, которую будем 
    забирать из артифактори. Представляет собой файл artifactory_model_tags.yml, пример такого файла:

        category_worker:
            classification_model: dev
            img_category_model: stable
            services_mapper_model: dev
        duplicate_worker:
            auto: latest
    
    Если такого файла нет, то он создастся. Для этого сначала будет составлен список всех моделей-наследников 
    класса AbstractTrainedModel (подробнее про иерархию и архитектуру моделей, зависящих от внешних данных 
    см. https://cf..ru/pages/viewpage.action?pageId=42338220), далее для них будет взят тег 'latest'.
    
    Если файл уже имеется, то произойдет его обновление. Для этого будет составлен актуальный список моделей 
    как выше, после чего в таблицу будут добавлены отсутствующие в ней модели с тегом 'latest'. Уже имеющиеся
    в таблице модели и их теги изменены НЕ БУДУТ.

    """
    worker_to_registry_mapping = worker_to_registry_mapping or build_worker_to_registry_mapping()

    new_table = {
        worker: {
            model_name: 'latest' for model_name in registry.external_source_dependent_models
        } for worker, registry in worker_to_registry_mapping.items()
    }

    if os.path.exists(ARTIFACTORY_MODEL_TAGS_TABLE_PATH):
        with open(ARTIFACTORY_MODEL_TAGS_TABLE_PATH) as f:
            old_table = yaml.load(f.read())
    else:
        old_table = {}

    _table = defaultdict(dict)
    # добавляем недостающие ключи из новой таблицы, НЕ ИЗМЕНЯЯ СТАРЫХ
    # и удаляем ключи, которых нету в новой
    for worker, model_to_tag_mapping in new_table.items():
        for model_name, new_tag in model_to_tag_mapping.items():
            _table[worker][model_name] = old_table.get(worker, {}).get(model_name, new_tag)

    with open(ARTIFACTORY_MODEL_TAGS_TABLE_PATH, 'w') as f:
        yaml.dump(new_table, stream=f, indent=4, default_flow_style=False)

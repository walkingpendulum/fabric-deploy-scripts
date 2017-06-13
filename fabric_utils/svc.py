# coding=utf-8
import json
import os
from collections import defaultdict, namedtuple

import requests
import yaml
from fabric import api
from requests.auth import HTTPBasicAuth

from default_settings import artifactory as artifactory_settings
from fabric_utils.models import build_worker_to_registry_mapping
from fabric_utils.paths import ARTIFACTORY_MODEL_TAGS_TABLE_PATH, GIT_ROOT, DATA_PATH


class GitTreeHandler(object):
    repo_url = 'repo_url'

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

    @staticmethod
    def clone(path, git_ref):
        if git_ref.branch:
            with api.hide('output'):
                api.sudo('git clone -b {git_ref.branch} {repo_url} {path}'.format(
                    repo_url=GitTreeHandler.repo_url, path=path, git_ref=git_ref
                ))
        elif git_ref.commit:
            with api.hide('output'):
                api.sudo('git clone {git.repo_url} {path}'.format(
                    git=GitTreeHandler, path=path
                ))
                with api.cd(path):
                    api.run('git checkout --detach {.commit}'.format(git_ref))
        else:
            raise RuntimeError

    @staticmethod
    def force_pull(path, git_ref):
        if git_ref.branch:
            with api.cd(path):
                api.sudo('git fetch --all')
                api.sudo('git reset --hard origin/{.branch}'.format(git_ref))
        elif git_ref.commit:
            with api.cd(path):
                api.sudo('git fetch --all')
                api.run('git reset --hard HEAD')
                api.run('git checkout --detach {.commit}'.format(git_ref))
        else:
            raise RuntimeError


class ArtifactoryTreeHandler(object):
    prefix = 'http://artifactory'
    api_prefix = '%s/api/storage/service-local' % prefix
    repo_prefix = '%s/service-local' % prefix
    info_file = 'artifactory_info.json'

    @staticmethod
    def _get(url):
        auth = HTTPBasicAuth(**artifactory_settings['credentials'])
        resp = requests.get(url, auth=auth)

        return resp

    @staticmethod
    def load(uri):
        url = '%s/%s' % (ArtifactoryTreeHandler.repo_prefix, uri)
        resp = ArtifactoryTreeHandler._get(url)

        return resp

    @staticmethod
    def info(uri):
        url = '%s/%s' % (ArtifactoryTreeHandler.api_prefix, uri)
        resp = ArtifactoryTreeHandler._get(url).json()

        return resp

    @staticmethod
    def check_if_loading_needed(uri, local_path_obj):
        try:
            with open(os.path.join(local_path_obj.folder, ArtifactoryTreeHandler.info_file)) as f:
                old_info = json.load(f)
        except (IOError, OSError):
            return True

        info = ArtifactoryTreeHandler.info(uri)

        return info['lastUpdated'] > old_info['lastUpdated']

    @staticmethod
    def dump_info(uri, local_path_obj):
        info = ArtifactoryTreeHandler.info(uri)
        with open(os.path.join(local_path_obj.folder, ArtifactoryTreeHandler.info_file), 'w') as f:
            json.dump(obj=info, fp=f)

    @staticmethod
    def invalidate_cache():
        api.run('find %s -name "%s" -type f -delete' % (DATA_PATH, ArtifactoryTreeHandler.info_file))


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
        yaml.dump(_table, stream=f, indent=4, default_flow_style=False)

# coding: utf-8
from __future__ import print_function

import glob
import importlib
import os
import sys
from collections import namedtuple, defaultdict
from functools import wraps

import yaml
from fabric import api
from fabric.colors import green, red
from fabric.context_managers import cd
from fabric.decorators import task

from fabric_utils import artifactory
from fabric_utils import local
from fabric_utils.rabbit import manage_rabbitmq, wait_rabbit_for_start


GIT_ROOT = os.getenv('GIT_ROOT', os.path.abspath('.'))
DATA_PATH = os.path.join(GIT_ROOT, 'data')
ARTIFACTORY_MODEL_TAGS_TABLE_PATH = 'artifactory_model_tags.yml'

sys.path.insert(0, GIT_ROOT)

_LocalPath = namedtuple('LocalPath', ['folder', 'file'])

api.env.use_ssh_config = True
api.env.sudo_user = 'user'


# noinspection PyPep8Naming
def LocalPath(folder, file=None):
    return _LocalPath(folder, file)


class all_hosts_container(object):
    _ids = '01 05 06 07 s01 s02 s03 s04 s05 s06 s07 s08 s09 s10'

    @classmethod
    def get_host(cls, id_):
        if id_.startswith('s'):
            host = 'server{}'.format(id_[len('s'):])
        else:
            host = 'gserver{}'.format(id_)

        return host

    @classmethod
    def get_hosts(cls, *selectors):
        if selectors:
            selectors = set(selectors).__contains__
        ids = filter(selectors or None, cls._ids.split(' '))

        hosts = map(cls.get_host, ids)
        return hosts


all_hosts = all_hosts_container.get_hosts


def task_with_hosts(task_):
    @task
    @wraps(task_)
    def enhanced_with_hosts_task(*selectors):
        hosts = all_hosts(*selectors)
        if not hosts:
            raise AssertionError('There are no hosts stayed after hosts processing routine, please check hosts input')
        api.env.hosts = hosts
        api.execute(task_)

    return enhanced_with_hosts_task


def error_print(msg, **kwargs):
    print(red(msg), **kwargs)


def with_cd_to(path):
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            with cd(path):
                res = func()
                return res
        return wrapped
    return decorator


def with_cd_to_git_root(func):
    return with_cd_to(GIT_ROOT)(func)


@task
def up():
    local('docker-compose up -d rabbit')
    wait_rabbit_for_start()
    manage_rabbitmq()
    local('docker-compose up service')


@task
def clean():
    local('docker-compose down')


@task_with_hosts
def clone_repo():
    _clone_repo()


def _clone_repo(path=GIT_ROOT, url='repo_url'):
    api.run('mkdir -p %s' % path)
    assert len(path) > 5
    api.run('rm -rf %s' % path)

    api.sudo('git clone {url} {path}'.format(url=url, path=path))


@task_with_hosts
@with_cd_to_git_root
def update():
    api.sudo('git pull origin master')


@task_with_hosts
@with_cd_to_git_root
def run():
    _run()


def _run():
    _cmd = 'OMP_NUM_THREADS=1 nohup python service.py &> logs.txt &'
    cmd = "bash -c '%s'" % _cmd
    api.run(cmd, pty=False)


@task_with_hosts
def stop():
    _stop()


def _stop():
    with api.warn_only():
        template = 'curl --silent "http://%s:8888/%s" > /dev/null'
        for cmd in 'stop quit'.split():
            api.local(template % (api.env.host_string, cmd))


@task_with_hosts
def force_stop():
    api.sudo('pkill --signal 9 -f "^python service.py"')


@task_with_hosts
@with_cd_to_git_root
def error():
    count = api.sudo('grep ERROR logs.txt | wc -l')
    msg = 'On {} counted {} errors'.format(api.env.host_string, count)
    error_print(msg)


@task_with_hosts
def check():
    current_host = api.env.host_string
    host_url = '"http://%s:8888"' % current_host
    cmd = "curl -I %s 2>/dev/null | head -n 1 | cut -d$' ' -f2" % host_url
    code = api.local(cmd, capture=True)

    if code == '200':
        msg = '%s is alive' % current_host
        print(green(msg))
    else:
        msg = '%s is not working' % current_host
        error_print(msg)


def _load_vertica_driver():
    url = '%s/libs/vertica/libverticaodbc.so.7.1.1' % artifactory.ARTIFACTORY_PREFIX
    folder = os.path.abspath(os.path.join('.', 'data'))
    file_name = 'libverticaodbc.so.7.1.1'
    local_path_obj = LocalPath(folder, file_name)
    (response, exception), = artifactory.load_artifacts([(url, local_path_obj)])
    if exception or response.status_code != 200:
        msg = 'Vertica driver download failed! %s' % ' '.join([str(response), exception or ''])
        error_print(msg)


def _build_worker_to_registry_mapping():
    packages = glob.glob(os.path.join('service', '*', 'models'))
    worker_to_models_mapping = {}
    for path in packages:
        _, worker_name, _ = path.rsplit('/', 2)
        module = importlib.import_module(path.replace('/', '.'))
        worker_to_models_mapping[worker_name] = module.models

    return worker_to_models_mapping


def _collect_tasks_for_models_loading():
    worker_to_registry_mapping = _build_worker_to_registry_mapping()
    _update_tags_table(worker_to_registry_mapping)

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


def _load_models_data():
    tasks = _collect_tasks_for_models_loading()
    task_results = artifactory.load_artifacts(tasks)
    failed_results = filter(lambda (resp, exc): exc or resp.status_code != 200, task_results)
    if failed_results:
        _msg = lambda r, e: 'Download {.url} failed with {details}'.format(r, details=' '.join([str(r), e or '']))
        msg = '\n'.join([_msg(*result) for result in failed_results])
        error_print(msg)


def _load_features():
    url = '%s/features/Data-latest.tar.gz' % artifactory.ARTIFACTORY_PREFIX
    folder = os.path.abspath(os.path.join('.', 'data', 'Data'))
    local_path_obj = LocalPath(folder)
    (response, exception), = artifactory.load_artifacts([(url, local_path_obj)])
    if exception or response.status_code != 200:
        msg = '"Data/" download failed! %s' % ' '.join([str(response), exception or ''])
        error_print(msg)


def _load_wordforms():
    url = '%s/common/wordforms-latest.tar.gz' % artifactory.ARTIFACTORY_PREFIX
    folder = os.path.abspath(os.path.join('.', 'data', 'common', 'wordforms'))
    local_path_obj = LocalPath(folder)
    (response, exception), = artifactory.load_artifacts([(url, local_path_obj)])
    if exception or response.status_code != 200:
        msg = '"common/wordforms" download failed! %s' % ' '.join([str(response), exception or ''])
        error_print(msg)


def _update_tags_table(worker_to_registry_mapping=None):
    worker_to_registry_mapping = worker_to_registry_mapping or _build_worker_to_registry_mapping()

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


@task
@with_cd_to_git_root
def load_artifacts():
    if not os.path.exists(ARTIFACTORY_MODEL_TAGS_TABLE_PATH):
        _update_tags_table()

    _load_vertica_driver()
    _load_features()
    _load_wordforms()
    _load_models_data()
    with api.cd(GIT_ROOT):
        api.sudo('chown -R {user} {folder}'.format(user=api.env.sudo_user, folder=DATA_PATH))


@task_with_hosts
def deploy():
    _stop()
    _clone_repo()

    if os.path.exists(ARTIFACTORY_MODEL_TAGS_TABLE_PATH):
        api.put(ARTIFACTORY_MODEL_TAGS_TABLE_PATH, GIT_ROOT)

    with api.cd(GIT_ROOT):
        api.run('fab load_artifacts')

    _run()

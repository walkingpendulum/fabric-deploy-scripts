from __future__ import print_function

import glob
import importlib
import os
import sys
from collections import namedtuple
from functools import wraps

from fabric import api
from fabric.colors import green, red
from fabric.context_managers import cd
from fabric.decorators import task

from fabric_utils import artifactory
from fabric_utils import local
from fabric_utils.rabbit import manage_rabbitmq, wait_rabbit_for_start


GIT_ROOT = os.getenv('GIT_ROOT', '/var/local/service')
DATA_PATH = os.path.join(GIT_ROOT, 'data')

sys.path.insert(0, GIT_ROOT)

_LocalPath = namedtuple('LocalPath', ['folder', 'file'])


# noinspection PyPep8Naming
def LocalPath(folder, file=None):
    return _LocalPath(folder, file)


api.env.use_ssh_config = True
api.env.sudo_user = 'user'


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


@task
def all_hosts():
    ids = '01 05 06 07 s01 s02 s03 s04 s05 s06 s07 s08 s09 s10'.split(' ')
    api.env.hosts = []
    for id_ in ids:
        if id_.startswith('s'):
            host = 'server{}'.format(id_[len('s'):])
        else:
            host = 'gserver{}'.format(id_)
        api.env.hosts.append(host)


@task
def clone_repo(path=GIT_ROOT.rstrip('/'), url='repo_url'):
    api.run('mkdir -p %s' % os.path.dirname(path.rstrip('/')))
    api.sudo('git clone {url} {path}'.format(url=url, path=path))


@task
@with_cd_to_git_root
def update():
    api.sudo('git pull origin master')


@task
@with_cd_to_git_root
def run():
    api.sudo("bash -c 'OMP_NUM_THREADS=1 nohup python service.py > %s/logs.txt 2>&1&'" % GIT_ROOT)


@task
@with_cd_to_git_root
def stop():
    template = 'curl --silent "http://%s:8888/%s" > /dev/null'
    for cmd in 'stop quit'.split():
        api.local(template % (api.env.host_string, cmd))


@task
@with_cd_to_git_root
def force_stop():
    api.sudo('pkill --signal 9 -f "^python service.py"')


@task
@with_cd_to_git_root
def error():
    count = api.sudo('grep ERROR logs.txt | wc -l')
    msg = 'On {} counted {} errors'.format(api.env.host_string, count)
    error_print(msg)


@task
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


def _build_worker_to_models_mapping():
    packages = glob.glob(os.path.join('service', '*', 'models'))
    worker_to_models_mapping = {}
    for path in packages:
        _, worker_name, _ = path.rsplit('/', 2)
        module = importlib.import_module(path.replace('/', '.'))
        worker_to_models_mapping[worker_name] = module.models

    return worker_to_models_mapping


def _collect_tasks_for_models_loading():
    worker_to_models_mapping = _build_worker_to_models_mapping()
    tasks = []
    for worker, models_registry in worker_to_models_mapping.items():
        for model_name, tag in models_registry.tags.items():
            cls = models_registry[model_name]
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


@task
@with_cd_to_git_root
def load_artifacts():
    _load_vertica_driver()
    _load_features()
    _load_models_data()

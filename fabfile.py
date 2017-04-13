from __future__ import print_function

import os
from functools import wraps

from fabric import api
from fabric.colors import green, red
from fabric.context_managers import cd
from fabric.decorators import task

from fabric_utils import local
from fabric_utils.rabbit import manage_rabbitmq, wait_rabbit_for_start


GIT_ROOT = '/var/local/service'
api.env.use_ssh_config = True
api.env.sudo_user = 'user'


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
    return with_cd_to(GIT_ROOT)


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
    print(red(msg))


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
        print(red(msg))

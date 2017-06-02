# coding: utf-8
from __future__ import print_function

import collections
import os

from fabric import api
from fabric.colors import green, red
from fabric.decorators import task, parallel

import fabric_utils.artifactory as artifactory
from fabric_utils.context_managers import with_cd_to_git_root
from fabric_utils.decorators import task_with_shortened_hosts, get_hosts_from_shorts
from fabric_utils.delivery_tasks import collect_tasks
from fabric_utils.paths import GIT_ROOT, ARTIFACTORY_MODEL_TAGS_TABLE_PATH
from fabric_utils.svc import GitTreeHandler as git, update_tags_table as update_tags_table_routine

api.env.use_ssh_config = True
api.env.sudo_user = 'user'


@task
def set_branch(branch):
    api.env.git_branch = branch


def error_print(msg, **kwargs):
    print(red(msg), **kwargs)


@task_with_shortened_hosts
def clone_repo():
    _clone_repo()


def _clone_repo(path=GIT_ROOT, url='repo_url', branch=None):
    api.sudo('mkdir -p %s' % path)
    assert len(path) > 5
    api.sudo('rm -rf %s' % path)

    branch = branch or api.env.git_branch or 'master'
    with api.hide('output'):
        api.sudo('git clone -b {branch} {url} {path}'.format(url=url, path=path, branch=branch))


@task_with_shortened_hosts
@with_cd_to_git_root
def update():
    api.sudo('git pull origin master')


@task_with_shortened_hosts
@with_cd_to_git_root
def run():
    _run()


@with_cd_to_git_root
def _run():
    _cmd = 'OMP_NUM_THREADS=1 nohup python service.py &> logs.txt &'
    cmd = "bash -c '%s'" % _cmd
    api.sudo(cmd, pty=False)


@task_with_shortened_hosts
def force_stop():
    _force_stop()


def _force_stop():
    with api.settings(warn_only=True):
        api.sudo('pkill --signal 9 -f "^python service.py"')


@task_with_shortened_hosts
@with_cd_to_git_root
def error():
    count = api.sudo('grep ERROR logs.txt | wc -l')
    msg = 'On {} counted {} errors'.format(api.env.host_string, count)
    error_print(msg)


@task_with_shortened_hosts
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


@task
@with_cd_to_git_root
def load_artifacts():
    if not os.path.exists(ARTIFACTORY_MODEL_TAGS_TABLE_PATH):
        update_tags_table_routine()

    tasks = collect_tasks()
    task_results = artifactory.load_artifacts(tasks)
    failed_results = filter(lambda (resp, exc): exc or resp.status_code != 200, task_results)
    if failed_results:
        _msg = lambda r, e: 'Download {.url} failed with {details}'.format(
            r, details=' '.join([str(r), str(e) if e else ''])
        )
        msg = '\n'.join([_msg(*result) for result in failed_results])
        error_print(msg)


@task
def update_tags_table():
    update_tags_table_routine()


@task_with_shortened_hosts
def deploy():
    _force_stop()
    _clone_repo()

    if os.path.exists(ARTIFACTORY_MODEL_TAGS_TABLE_PATH):
        api.put(ARTIFACTORY_MODEL_TAGS_TABLE_PATH, GIT_ROOT)

    with api.cd(GIT_ROOT):
        api.sudo('fab load_artifacts')
        _run()


def _render_git_info(host_to_info_str_mapping, host_to_dirty_index_flag):
    info = collections.defaultdict(list)
    for host, info_str in host_to_info_str_mapping.items():
        info[info_str].append(host)

    dirty_postfix = lambda x: '(changes not staged for commit presented)' if host_to_dirty_index_flag[x] else ''
    render_line = lambda host: ' '.join([host, dirty_postfix(host)])

    for info_str, _hosts in info.items():
        _sorted_hosts_lines = sorted(_hosts, key=lambda h: (host_to_dirty_index_flag[h], h))
        hosts_lines = '\n\t'.join([render_line(line) for line in _sorted_hosts_lines])

        msg = (
            "Hosts:\n\t"
                "{hosts_lines}\n"
            "Git info:\n\t"
                "{git_info}\n"
        ).format(
            hosts_lines=hosts_lines,
            git_info=info_str
        )

        print(msg)


@task
def code_version(*selectors):
    hosts_to_run = get_hosts_from_shorts(selectors)

    @task
    @parallel
    def _code_version():
        info_str = git.info()
        return info_str

    @task
    @parallel
    def _git_dirty_index():
        flag = not git.is_index_empty()
        return flag

    with api.hide('everything'):
        host_to_info_str_mapping = api.execute(_code_version, hosts=hosts_to_run)
        host_to_dirty_index_flag = api.execute(_git_dirty_index, hosts=hosts_to_run)

    _render_git_info(host_to_info_str_mapping, host_to_dirty_index_flag)

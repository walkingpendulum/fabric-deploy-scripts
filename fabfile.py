# coding: utf-8
from __future__ import print_function

import collections
import os

from fabric import api
from fabric.colors import green, red
from fabric.decorators import task, parallel, serial

import fabric_utils.artifactory_loader as artifactory_loader
from fabric_utils.context_managers import with_cd_to_git_root
from fabric_utils.decorators import task_with_shortened_hosts, get_hosts_from_shorts
from fabric_utils.delivery_tasks import collect_tasks
from fabric_utils.paths import GIT_ROOT, ARTIFACTORY_MODEL_TAGS_TABLE_PATH
from fabric_utils.svc import ArtifactoryTreeHandler as artifactory
from fabric_utils.svc import GitTreeHandler as git
from fabric_utils.svc import update_tags_table as update_tags_table_routine
from fabric_utils.utils import GitRef


api.env.use_ssh_config = True
api.env.sudo_user = 'user'


@task
def set_git_ref(**kwargs):
    api.env.git_ref = GitRef(**kwargs)


def error_print(msg, **kwargs):
    print(red(msg), **kwargs)


@task_with_shortened_hosts
def clone_repo():
    _clone_or_pull_repo()


def _clone_or_pull_repo(path=GIT_ROOT, git_ref=None):
    git_ref = git_ref or api.env.get('git_ref', GitRef('master'))

    with api.hide('output'):
        with api.settings(warn_only=True):
            path_exists = not api.run('test -d %s' % path).return_code
        if not path_exists:
            git.clone(path, git_ref)
        else:
            git.force_pull(path, git_ref)


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
    task_results = artifactory_loader.load_artifacts(tasks)
    failed_results = filter(lambda (resp, exc): exc or resp.status_code != 200, task_results)
    if failed_results:
        _msg = lambda r, e: 'Download {.url} failed with {details}'.format(
            r, details=' '.join([str(r), str(e) if e else ''])
        )
        msg = '\n'.join([_msg(*result) for result in failed_results])
        error_print(msg)


@task_with_shortened_hosts
def invalidate_artifactory_cache():
    artifactory.invalidate_cache()


@task
def update_tags_table():
    update_tags_table_routine()


def _put_local_tag_table_if_exists():
    file_name = os.path.basename(ARTIFACTORY_MODEL_TAGS_TABLE_PATH)
    if os.path.exists(file_name):
        api.put(file_name, GIT_ROOT)


@task_with_shortened_hosts
def deploy():
    _force_stop()
    _clone_or_pull_repo()

    _put_local_tag_table_if_exists()
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
@serial
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


@task
def grep(grep_str=''):
    if grep_str:
        api.env.grep_str = grep_str


@task
@serial
def tail(*selectors):
    """
        Usage:
            $ fab grep:'-v "INFO\|WARNING"' tail:all,x01,x07

    """

    @task
    @parallel
    def _tail(filename):
        cmd = 'tail -f %s' % filename

        try:
            grep_str = api.env.grep_str
            cmd = "%s | grep %s" % (cmd, grep_str)
        except AttributeError:
            pass

        try:
            api.run(cmd)
        except (KeyboardInterrupt, IOError):
            pass

    hosts = get_hosts_from_shorts(selectors)
    log_file_path = '/var/local/service/logs.txt'

    with api.settings(linewise=True):
        api.execute(_tail, log_file_path, hosts=hosts)


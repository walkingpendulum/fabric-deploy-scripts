# coding: utf-8
from __future__ import print_function

import collections
import datetime

from fabric import api
from fabric.decorators import task, parallel

from fabric_utils.context_managers import with_cd_to_git_root
from fabric_utils.paths import GIT_ROOT
from fabric_utils.patterns import kill_service_regex
from fabric_utils.svc import GitTreeHandler as git
from fabric_utils.utils import GitRef

api.env.use_ssh_config = True
api.env.sudo_user = 'user'


@with_cd_to_git_root
def run_service_script(script):
    _cmd = 'OMP_NUM_THREADS=1 nohup python %s &> logs.txt &' % script
    cmd = "bash -c '%s'" % _cmd
    api.sudo(cmd, pty=False)


def force_stop_service_process():
    with api.settings(warn_only=True):
        api.sudo('pkill --signal 9 -f "%s"' % kill_service_regex)


def deploy_service(executable_script='service.py'):
    force_stop_service_process()
    clone_or_pull_service_repo()

    with api.cd(GIT_ROOT):
        api.sudo('fab load_artifacts')
        run_service_script(executable_script)


def render_git_info(host_to_git_info_str_mapping, host_to_dirty_index_flag, host_to_readiness_flag,
                    host_to_probable_last_deploy_time):
    """git + readiness status + deploy time

    :param host_to_git_info_str_mapping:
    :param host_to_dirty_index_flag:
    :param host_to_readiness_flag:
    :param host_to_probable_last_deploy_time:
    :return:
    """
    info = collections.defaultdict(list)
    for host, info_str in host_to_git_info_str_mapping.items():
        info[info_str].append(host)

    dirty_postfix = lambda x: '(changes not staged for commit presented)' if host_to_dirty_index_flag[x] else ''
    readiness_status = lambda x: '(NOT READY)' if not host_to_readiness_flag[x] else ''
    deploy_time = lambda x, d=host_to_probable_last_deploy_time: 'last deploy: %s' % d[x].ctime() if d[x] else ''

    render_line = lambda host: ' '.join([host, dirty_postfix(host), deploy_time(host), readiness_status(host)])

    for info_str, _hosts in info.items():
        _sorted_hosts_lines = sorted(_hosts, key=lambda h: (host_to_readiness_flag[h], host_to_dirty_index_flag[h], h))
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


def clone_or_pull_service_repo(path=GIT_ROOT, git_ref=None):
    git_ref = git_ref or api.env.get('git_ref', GitRef('master'))

    with api.hide('output'):
        with api.settings(warn_only=True):
            path_exists = not api.run('test -d %s' % path).return_code
        if not path_exists:
            git.clone(path, git_ref)
        else:
            git.force_pull(path, git_ref)


def readiness_probe(hosts_to_run):

    @task
    @parallel
    def _is_alive():
        with api.settings(warn_only=True):
            cmd = "import requests; print requests.get('http://localhost:9888/health').status_code == requests.codes.ok"
            return api.run('python -c "%s"' % cmd) == 'True'

    with api.hide('everything'):
        host_to_flags = api.execute(_is_alive, hosts=hosts_to_run)

    return host_to_flags


def probable_last_deploy_time(hosts_to_run):
    # тут используется хак, который однажды может перестать работать

    @task
    @parallel
    def _probable_last_deploy_time():
        with api.settings(warn_only=True):
            cmd = "import os; print os.path.getmtime('%s/global_settings.pyc')" % GIT_ROOT
            try:
                ts = int(float(api.run('python -c "%s"' % cmd)))
                return datetime.datetime.fromtimestamp(ts)
            except Exception:
                return

    with api.hide('everything'):
        host_to_probable_last_deploy_time = api.execute(_probable_last_deploy_time, hosts=hosts_to_run)

    return host_to_probable_last_deploy_time

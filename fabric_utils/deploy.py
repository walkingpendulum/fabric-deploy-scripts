from __future__ import print_function

import collections
import os

from fabric import api
from fabric.colors import red

from fabric_utils.context_managers import with_cd_to_git_root
from fabric_utils.paths import GIT_ROOT, ARTIFACTORY_MODEL_TAGS_TABLE_PATH
from fabric_utils.patterns import kill_service_regex
from fabric_utils.svc import GitTreeHandler as git
from fabric_utils.utils import GitRef

api.env.use_ssh_config = True
api.env.sudo_user = 'user'


@with_cd_to_git_root
def _run(script):
    _cmd = 'OMP_NUM_THREADS=1 nohup python %s &> logs.txt &' % script
    cmd = "bash -c '%s'" % _cmd
    api.sudo(cmd, pty=False)


def _force_stop():
    with api.settings(warn_only=True):
        api.sudo('pkill --signal 9 -f "%s"' % kill_service_regex)


def _deploy(script='service.py'):
    _force_stop()
    _clone_or_pull_repo()

    _put_local_tag_table_if_exists()
    with api.cd(GIT_ROOT):
        api.sudo('fab load_artifacts')
        _run(script)


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


def _put_local_tag_table_if_exists():
    file_name = os.path.basename(ARTIFACTORY_MODEL_TAGS_TABLE_PATH)
    if os.path.exists(file_name):
        api.put(file_name, GIT_ROOT)


def _clone_or_pull_repo(path=GIT_ROOT, git_ref=None):
    git_ref = git_ref or api.env.get('git_ref', GitRef('master'))

    with api.hide('output'):
        with api.settings(warn_only=True):
            path_exists = not api.run('test -d %s' % path).return_code
        if not path_exists:
            git.clone(path, git_ref)
        else:
            git.force_pull(path, git_ref)


def error_print(msg, **kwargs):
    print(red(msg), **kwargs)

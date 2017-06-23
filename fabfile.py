# coding: utf-8
from __future__ import print_function

import os

from fabric import api
from fabric.colors import green, red
from fabric.decorators import task, parallel, serial

import fabric_utils.artifactory_loader as artifactory_loader
from fabric_utils.context_managers import with_cd_to_git_root
from fabric_utils.decorators import task_with_shortened_hosts, get_hosts_from_shorts
from fabric_utils.delivery_tasks import collect_tasks
from fabric_utils.deploy import _run, _force_stop, _deploy, _render_git_info, _clone_or_pull_repo, error_print
from fabric_utils.paths import ARTIFACTORY_MODEL_TAGS_TABLE_PATH
from fabric_utils.svc import ArtifactoryTreeHandler as artifactory
from fabric_utils.svc import GitTreeHandler as git
from fabric_utils.svc import update_tags_table as update_tags_table_routine
from fabric_utils.utils import GitRef
from default_settings import disposer_settings
from fabric_utils.patterns import kill_service_regex

api.env.use_ssh_config = True
api.env.sudo_user = 'user'


@task
def set_git_ref(**kwargs):
    """Устанавливает переменную окружения git_ref, которая затем используется в deploy тасках"""
    api.env.git_ref = GitRef(**kwargs)


@task_with_shortened_hosts
def clone_repo():
    """git clone или git reset --hard && git pull"""
    _clone_or_pull_repo()


@task_with_shortened_hosts
@with_cd_to_git_root
def update():
    """git pull origin master"""
    api.sudo('git pull origin master')


@task_with_shortened_hosts
@with_cd_to_git_root
def run():
    """Запускает service.py без деплоя или обновления)"""
    _run(script='service.py')


@task_with_shortened_hosts
def deploy():
    """Останавливет сервис, подтягивает обновления кода и моделей, запускает service.py"""
    _deploy(script='service.py')


@task_with_shortened_hosts
def deploy_autoload():
    """То же, что и deploy, но для service-autoload.py"""
    _deploy(script='service-autoload.py')


@task_with_shortened_hosts
def deploy_autoload_check():
    """То же, что и deploy, но для service-autoload-check.py"""
    _deploy(script='service-autoload-check.py')


@task_with_shortened_hosts
def force_stop():
    """pkill --signal 9 -f '%s'"""
    _force_stop()

force_stop.__doc__ %= kill_service_regex


@serial
@task
def error(*selectors):
    """Считает количество ошибок на хостах"""

    @task
    @parallel
    @with_cd_to_git_root
    def count():
        return api.run('grep ERROR logs.txt | wc -l')

    hosts = get_hosts_from_shorts(selectors)
    with api.hide('everything'):
        host_to_counter_mapping = api.execute(count, hosts=hosts)
    
    lines = []
    for host, counter in sorted(host_to_counter_mapping.items(), key=lambda (h, c): h):
        lines.append('On {} counted {} errors'.format(host, counter))

    print('\n'.join(lines))


@task
@serial
def check(*selectors):
    """Выполняет health check на хостах"""

    _opts = [
        '-s',
        '-I',
        '-o /dev/null',
        '-w "%{http_code}"'
    ]

    cmd = "curl {opts} {disposer[host]}:{disposer[port]}".format(
        opts=' '.join(_opts),
        disposer=disposer_settings['webserver']
    )

    @task
    @parallel
    def get_http_code():
        return api.sudo(cmd)

    hosts = get_hosts_from_shorts(selectors)
    with api.hide('everything'):
        host_to_code_mapping = api.execute(get_http_code, hosts=hosts)

    lines = []
    for host, code in sorted(host_to_code_mapping.items(), key=lambda (h, c): h):
        if code == '200':
            msg = green('%s is alive' % host)
            lines.append(msg)
        else:
            msg = red('%s is not working' % host)
            lines.append(msg)

    print('\n'.join(lines))


@task
@with_cd_to_git_root
def load_artifacts():
    """Загружает файлы из артифактори соогласно таблице тегов"""
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
    """Очищает кеш загрузок из артифактори"""
    artifactory.invalidate_cache()


@task
def update_tags_table():
    """Обновляет таблицу тегов"""
    update_tags_table_routine()


@task
@serial
def code_version(*selectors):
    """Собирает информацию о версии кода на хостах"""
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
    """Устанавливает переменную grep_str, которая затем используется в таске tail"""
    if grep_str:
        api.env.grep_str = grep_str


@task
@serial
def tail(*selectors):
    """multitail -f со всех хостов. может использоваться в связке с таской `grep`

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

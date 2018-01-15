# coding: utf-8
from __future__ import print_function

import importlib
import sys

from fabric import api
from fabric.decorators import task, parallel, serial

import artifactory.api
import fabric_utils.deploy
from fabric_utils.context_managers import with_cd_to_git_root
from fabric_utils.decorators import task_with_shortened_hosts, get_hosts_from_shorts
from fabric_utils.delivery_tasks import collect_tasks
from fabric_utils.paths import GIT_ROOT, DATA_PATH
from fabric_utils.patterns import kill_service_regex
from fabric_utils.rabbit import create_queues as create_queues_routine
from fabric_utils.svc import GitTreeHandler as git
from fabric_utils.utils import GitRef

api.env.use_ssh_config = True
api.env.sudo_user = 'user'


@task
def set_git_ref(**kwargs):
    """Устанавливает переменную окружения git_ref, которая затем используется в deploy тасках"""
    api.env.git_ref = GitRef(**kwargs)


@task_with_shortened_hosts
def clone_repo():
    """git clone или git reset --hard && git pull"""
    fabric_utils.deploy.clone_or_pull_service_repo()


@task_with_shortened_hosts
@with_cd_to_git_root
def update():
    """git pull origin master"""
    api.sudo('git pull origin master')


@task_with_shortened_hosts
@with_cd_to_git_root
def run():
    """Запускает service.py без деплоя или обновления)"""
    fabric_utils.deploy.run_service_script(script='service.py')


@task_with_shortened_hosts
def deploy():
    """Останавливет сервис, подтягивает обновления кода и моделей, запускает service.py"""
    fabric_utils.deploy.deploy_service(executable_script='service.py')


@task_with_shortened_hosts
def deploy_autoload():
    """То же, что и deploy, но для service-autoload.py"""
    fabric_utils.deploy.deploy_service(executable_script='service-autoload.py')


@task_with_shortened_hosts
def deploy_autoload_check():
    """То же, что и deploy, но для service-autoload-check.py"""
    fabric_utils.deploy.deploy_service(executable_script='service-autoload-check.py')


@task_with_shortened_hosts
def force_stop():
    """pkill --signal 9 -f '%s'"""
    fabric_utils.deploy.force_stop_service_process()


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
@with_cd_to_git_root
def load_artifacts(cred_str=None, worker_name_mask='worker'):
    """Загружает файлы из артифактори соогласно таблице тегов
    :param cred_str: credentials для артифактори в формате login:password
    :param worker_name_mask: маска для поиска папки с воркером, по умолчанию '*'
    """
    tasks = collect_tasks(worker_name_mask=worker_name_mask)

    if not cred_str:
        sys.path.append(GIT_ROOT)
        import global_settings
        cred_str = global_settings.artifactory_credentials_provider.provide_content()

    artifactory.api.execute_tasks(tasks, cred_str)


@task_with_shortened_hosts
def invalidate_artifactory_cache():
    """Очищает кеш загрузок из артифактори"""
    api.run('find %s -name "%s" -type f -delete' % (DATA_PATH, artifactory.api.info_file))


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

    fabric_utils.deploy.render_git_info(host_to_info_str_mapping, host_to_dirty_index_flag)


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


@task
def create_queues(rabbitmq_connection_string, path_to_job_settings):
    """Создает очереди в реббите по connection_string и relative/path/to/job/settings (без .py)"""
    # connection_string as 'ampq://{0[username]}:{0[password]}@{1[host]}:{1[port]:d}/{1[virtual_host]}'
    sys.path.append(GIT_ROOT)

    job_settings_module = importlib.import_module(path_to_job_settings.replace('/', '.'))

    for name in filter(lambda name: 'job_settings' in name, vars(job_settings_module)):
        job_settings = getattr(job_settings_module, name)
        create_queues_routine(rabbitmq_connection_string, job_settings)

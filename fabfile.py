# coding: utf-8
from __future__ import print_function

import importlib
import sys
import time
from itertools import izip_longest

from fabric import api
from fabric.decorators import task, parallel, serial

import artifactory.api
import fabric_utils.deploy
import fabric_utils.tasks
import fabric_utils.utils
from fabric_utils.context_managers import with_cd_to_git_root
from fabric_utils.decorators import task_with_shortened_hosts, get_hosts_from_shorts
from fabric_utils.delivery_tasks import collect_tasks
from fabric_utils.notifications import slack
from fabric_utils.paths import GIT_ROOT, DATA_PATH
from fabric_utils.patterns import kill_service_regex
from fabric_utils.rabbit import create_queues as create_queues_routine
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

        cred_dict = global_settings.artifactory_credentials_provider.provide_content()
        cred_str = '{0[username]}:{0[password]}'.format(cred_dict)

    artifactory.api.execute_tasks(tasks, cred_str)


@task_with_shortened_hosts
def invalidate_artifactory_cache():
    """Очищает кеш загрузок из артифактори"""
    api.run('find %s -name "%s" -type f -delete' % (DATA_PATH, artifactory.api.info_file))


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


@task
@serial
def status(*selectors):
    """Собирает информацию о версии кода на хостах + readiness status"""
    output = fabric_utils.deploy.status(*selectors)
    print(output)


@task
@serial
def rolling_deploy(*selectors):
    """Последовательный деплой сервиса на сервера

    Игнорирует хосты gserver0{5,6,7}! Используйте для них
        $ GIT_ROOT=/var/local/service fab deploy_autoload_check:05 deploy_autoload:06,07


    Хосты обрабатываются группами по 3. После деплоя на очередную группу хостов в цикле раз в 10 секунд
    проверяются readiness probe на каждом хосте. Деплой на группу хостов считается успешным, если все
    хосты успешно прошли readiness probe. Следующая группа хостов берется в работу только если предыдущая
    была успешно завершена.

    """
    hosts_to_skip = [
        'gserver05', 'gserver06', 'gserver07',   # это автозагрузочные
    ]
    hosts_to_run = get_hosts_from_shorts(selectors)
    hosts_to_run = filter(
        lambda host: all(to_skip_pattern not in host for to_skip_pattern in hosts_to_skip),
        hosts_to_run
    )

    waves_str = '~~~~~~~~~~~~~~~~~~~~'

    def grouper(n, iterable, fillvalue=None):
        "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
        args = [iter(iterable)] * n
        return izip_longest(fillvalue=fillvalue, *args)

    @task
    @serial
    def _deploy_task():
        fabric_utils.deploy.deploy_service(executable_script='service.py')

    for current_hosts_to_run in grouper(3, hosts_to_run):
        current_hosts_to_run = filter(None, current_hosts_to_run)
        if not current_hosts_to_run:
            break

        print('\n\n\n%s\n\tdeploy to %s\n%s' % (waves_str, ', '.join(current_hosts_to_run), waves_str))
        api.execute(_deploy_task, hosts=current_hosts_to_run)

        print('\n\t waiting for readiness probing.', end='')

        while True:
            time.sleep(10)
            print('.', end='')

            alive = fabric_utils.tasks.readiness_probe_task(current_hosts_to_run)
            if all(x is True for x in alive.values()):
                break

    try:
        output = fabric_utils.deploy.status('all')
        msg = 'service was deployed!\n```%s```' % output
        slack('#service-deploy', msg)
    except Exception as e:
        print("Can't send notification, skip this step.", file=sys.stderr)
        print(e, file=sys.stderr)

    print('\n%sSuccess!%s' % (waves_str, waves_str))

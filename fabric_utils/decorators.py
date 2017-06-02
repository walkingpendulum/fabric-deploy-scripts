# coding=utf-8
import sys
from functools import wraps

from fabric import api
from fabric.decorators import task

from fabric_utils.hosts import all_hosts_container


def get_hosts_from_shorts(selectors):
    decorator_name = sys._getframe().f_code.co_name
    assert not api.env.hosts, (
        '%s prevents from using underlying task with setted fabirc.api.env.hosts field. '
        'see docstring for details'
    ) % decorator_name

    hosts = all_hosts_container.get_hosts(*selectors)

    return hosts


def task_with_shortened_hosts(task_):
    """Декоратор, превращающий функцию в таску для fabric, допускающую задание хостов сокращениями.


    Usage:

    Например, если
        @task_with_shortened_hosts
        def f():
            api.run("echo 'hey!'")
        
    То вызов `fab f:s01,01` выполнит таску f на хостах, соответствующих s01 и 01.

    Другой пример, если
            @task_with_shortened_hosts
            def h():
                api.run("echo 'hey!'")
            
            @task
            def f():
                execute(h, 's01', '01')

    Тогда вызов `fab f` приведет к выполнению таски h на хостах, соответствующих s01 и 01 

    
    Restrictions:
        - таски, обернутые в этот декоратор, не должны принимать никаких входных параметров
        - декоратор делает невозможным использование таски, если поле hosts уже выставлено. иными словами:
            - флаги -H для этой таски больше НЕ РАБОТАЮТ, выполнение будет завершено
            - эту таску нельзя вызвать из другой таски с помощью execute, если какие-нибудь из предыдущих тасок 
            выставили поле hosts
    
    :param task_: 
    :return: 
    """

    @task
    @wraps(task_)
    def enhanced_with_hosts_task(*selectors):
        hosts = get_hosts_from_shorts(selectors)
        results = api.execute(task_, hosts=hosts)

        return results

    return enhanced_with_hosts_task

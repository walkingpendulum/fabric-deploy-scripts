# coding: utf-8
from fabric import api
from fabric.decorators import task, parallel

import fabric_utils.svc
import fabric_utils.utils


@task
@parallel
def dirty_git_index_task():
    flag = not fabric_utils.svc.GitTreeHandler.info()
    return flag


@task
@parallel
def code_version_task():
    info_str = fabric_utils.svc.GitTreeHandler.info()
    return info_str


def readiness_probe_task(hosts_to_run):

    @task
    @parallel
    def probe():
        return fabric_utils.utils.readiness_probe()

    with api.hide('everything'):
        host_to_flags = api.execute(probe, hosts=hosts_to_run)

    return host_to_flags

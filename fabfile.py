from __future__ import print_function

import json
import time

from fabric import api
from fabric.context_managers import shell_env, hide
from fabric.decorators import task

try:
    with open('docker.settings.json') as fp:
        _docker_settings = json.load(fp)
except IOError:
    _docker_settings = {}

env = {}
env.update(_docker_settings)


class ContainerFailedToStart(Exception):
    pass


def local(cmd, *args, **kwargs):
    with shell_env(**env):
        output = api.local(cmd, *args, **kwargs)

    return output


def create_vhost(vhost_name, credentials='guest:guest', rabbit_container_name='service_rabbit_1',
                 network='service_default'):
    _curl_opts = [
        '-i',
        '-u %s' % credentials,
        '-H "content-type:application/json"',
        '-X PUT',

    ]
    cmd = 'curl {opts} http://rabbit:15672/api/vhosts/{vhost} 2>/dev/null'.format(
        cred=credentials, vhost=vhost_name, opts=' '.join(_curl_opts),
    )
    local('docker run --rm --link {rabbit_container}:rabbit --network {network} service {cmd}'.format(
        rabbit_container=rabbit_container_name, cmd=cmd, network=network
    ))


def create_user(user_name='test', user_password='test', credentials='guest:guest',
                rabbit_container_name='service_rabbit_1', network='service_default'):
    _curl_opts = [
        '-i',
        '-u %s' % credentials,
        '-H "content-type:application/json"',
        '-X PUT',
        '-d \'{"password":"%s", "tags":"administrator"}\'' % user_password
    ]
    cmd = 'curl {opts} http://rabbit:15672/api/users/{username} 2>/dev/null'.format(
        cred=credentials, username=user_name, opts=' '.join(_curl_opts),
    )

    local('docker run --rm --link {rabbit_container}:rabbit --network {network} service {cmd}'.format(
        rabbit_container=rabbit_container_name, cmd=cmd, network=network
    ))


def grant_permission(user_name='test', vhost='test', credentials='guest:guest',
                rabbit_container_name='service_rabbit_1', network='service_default'):
    _curl_opts = [
        '-i',
        '-u %s' % credentials,
        '-H "content-type:application/json"',
        '-X PUT',
        '-d \'{"configure":".*", "write":".*", "read":".*"}\'',
    ]
    cmd = 'curl {opts} http://rabbit:15672/api/permissions/{vhost}/{user} 2>/dev/null'.format(
        cred=credentials, user=user_name, vhost=vhost, opts=' '.join(_curl_opts),
    )

    local('docker run --rm --link {rabbit_container}:rabbit --network {network} service {cmd}'.format(
        rabbit_container=rabbit_container_name, cmd=cmd, network=network
    ))


def manage_rabbitmq():
    create_user('test', 'test')
    create_vhost('dwh')
    grant_permission(user_name='test', vhost='dwh')


def wait_rabbit_for_start(container_name='service_rabbit_1', phrase='Server startup complete'):
    with hide('output', 'running'):
        sleep_time_sec = 5
        count = int(300. / sleep_time_sec)
        while True:
            output = local('docker logs %s' % container_name, capture=True)
            if phrase in output:
                break

            count -= 1
            if not count:
                print('Container %s failed to start, aborting' % container_name)
                raise ContainerFailedToStart

            print('Heartbeat... Waiting %s for start' % container_name)
            time.sleep(sleep_time_sec)


@task
def up():
    local('docker-compose up -d rabbit')
    wait_rabbit_for_start()
    manage_rabbitmq()
    local('docker-compose up service')


@task
def clean():
    local('docker-compose down')

import json

from fabric import api
from fabric.context_managers import shell_env


try:
    with open('docker.settings.json') as fp:
        _docker_settings = json.load(fp)
except IOError:
    _docker_settings = {}

env = {}
env.update(_docker_settings)


def local(cmd, *args, **kwargs):
    with shell_env(**env):
        output = api.local(cmd, *args, **kwargs)

    return output

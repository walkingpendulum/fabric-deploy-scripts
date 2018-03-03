from collections import namedtuple

from fabric import api

_GitRef = namedtuple('GitRef', ['branch', 'commit'])


# noinspection PyPep8Naming
def GitRef(branch=None, commit=None):
    return _GitRef(branch, commit)


def readiness_probe():
    with api.settings(warn_only=True):
        cmd = "import requests; print requests.get('http://localhost:9888/health').status_code == requests.codes.ok"
        return api.run('python -c "%s"' % cmd) == 'True'

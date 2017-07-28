# coding=utf-8
from functools import wraps

from fabric.context_managers import cd

from fabric_utils.paths import GIT_ROOT


def with_cd_to(path):
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            with cd(path):
                res = func()
                return res
        return wrapped
    return decorator


def with_cd_to_git_root(func):
    return with_cd_to(GIT_ROOT)(func)

# coding=utf-8

from fabric import api

from fabric_utils.paths import GIT_ROOT


class GitTreeHandler(object):
    repo_url = 'repo_url'

    @staticmethod
    def info():
        with api.cd(GIT_ROOT):
            api.sudo('git fetch')
            lines = api.run('git branch -vv')
            info_str_ = next(line[len('* '):] for line in lines.split('\n') if line.startswith('* '))

        return info_str_

    @staticmethod
    def is_index_empty():
        with api.cd(GIT_ROOT), api.settings(warn_only=True):
            result = api.run("git diff-files --quiet")

        return not result.return_code

    @staticmethod
    def clone(path, git_ref):
        if git_ref.branch:
            with api.hide('output'):
                api.sudo('git clone -b {git_ref.branch} {repo_url} {path}'.format(
                    repo_url=GitTreeHandler.repo_url, path=path, git_ref=git_ref
                ))
        elif git_ref.commit:
            with api.hide('output'):
                api.sudo('git clone {git.repo_url} {path}'.format(
                    git=GitTreeHandler, path=path
                ))
                with api.cd(path):
                    api.run('git checkout --detach {.commit}'.format(git_ref))
        else:
            raise RuntimeError

    @staticmethod
    def force_pull(path, git_ref):
        if git_ref.branch:
            with api.cd(path):
                api.sudo('git fetch --all')
                api.sudo('git reset --hard origin/{.branch}'.format(git_ref))
        elif git_ref.commit:
            with api.cd(path):
                api.sudo('git fetch --all')
                api.run('git reset --hard HEAD')
                api.run('git checkout --detach {.commit}'.format(git_ref))
        else:
            raise RuntimeError

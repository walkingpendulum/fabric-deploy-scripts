from collections import namedtuple


_GitRef = namedtuple('GitRef', ['branch', 'commit'])
_LocalPath = namedtuple('LocalPath', ['folder', 'file'])


# noinspection PyPep8Naming
def GitRef(branch=None, commit=None):
    return _GitRef(branch, commit)


# noinspection PyPep8Naming
def LocalPath(folder, file_=None):
    return _LocalPath(folder, file_)

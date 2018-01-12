from collections import namedtuple


_GitRef = namedtuple('GitRef', ['branch', 'commit'])


# noinspection PyPep8Naming
def GitRef(branch=None, commit=None):
    return _GitRef(branch, commit)

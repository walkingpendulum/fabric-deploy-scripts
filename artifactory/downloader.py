import json
import multiprocessing.dummy
import os
import tarfile
from StringIO import StringIO

import requests

from artifactory.logger import make_logger

info_file = 'artifactory_info.json'


def no_need_to_reload(folder, info_url, auth):
    try:
        with open(os.path.join(folder, info_file)) as f:
            old_info = json.load(f)
    except (IOError, OSError):
        return

    info = requests.get(info_url, auth=auth).json()
    return info['lastUpdated'] <= old_info['lastUpdated']


def dump_info(info, folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

    with open(os.path.join(folder, info_file), 'w') as f:
        json.dump(obj=info, fp=f)


def put_file(resp, folder, file=None):
    url = resp.url

    if '/' in url and '.tar.gz' in url.rsplit('/')[-1]:
        # tar archive
        tar = tarfile.open(fileobj=StringIO(resp.content), mode="r:gz")
        tar.extractall(folder)
        tar.close()
    else:
        assert file is not None
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.args[0] == 17:
                # `already exists` code
                pass
            else:
                raise
        path = os.path.join(folder, file)
        with open(path, 'wb') as f:
            resp.raw.decode_content = True
            f.write(resp.content)


def execute_download_task(task, auth=None):
    """
    :param task:
    :param auth:
    :return: tuple (url, smth), where smth or is belongs to {'success', 'skipped'} either is Exception object

    """
    logger = make_logger('artifactory-cli-downloader')

    folder = task['destination'].get('folder', os.path.abspath('.'))
    url = task['url']
    info_url = task.get('info_url')

    flag = no_need_to_reload(folder, info_url, auth) if info_url else None
    if not flag:
        try:
            resp = requests.get(url, auth=auth)
            put_file(resp, **task['destination'])

            info = requests.get(info_url, auth=auth).json()
            dump_info(info, folder)
            logger.info("Finish task %s with %s" % (url, resp))

            return url, 'success'

        except Exception as exception:
            return url, exception
    else:
        return url, 'skipped'


def execute_download_tasks(tasks_list, auth=None):
    pool = multiprocessing.dummy.Pool()
    results = pool.map(lambda task: execute_download_task(task, auth=auth), tasks_list)

    return results

import os
import tarfile
from StringIO import StringIO
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as ThreadPool

import requests
from requests.auth import HTTPBasicAuth

from default_settings import artifactory as artifactory_settings

ARTIFACTORY_PREFIX = os.getenv('SERVICE_ARTIFACTORY_PREFIX', 'http://artifactory/service-local')


def get_num_threads():
    default = min(15, int(cpu_count() / 2))
    return os.getenv('SERVICE_NUM_THREADS', default)


def _put(resp, local_path_obj):
    if resp.status_code != 200:
        return

    url = resp.url

    try:
        if '/' in url and '.tar.gz' in url.rsplit('/')[-1]:
            # tar archive
            tar = tarfile.open(fileobj=StringIO(resp.content), mode="r:gz")
            tar.extractall(local_path_obj.folder)
            tar.close()
        else:
            assert local_path_obj.file is not None
            try:
                os.makedirs(local_path_obj.folder)
            except OSError as e:
                if e.args[0] == 17:
                    # `already exists` code
                    pass
                else:
                    raise
            path = os.path.join(local_path_obj.folder, local_path_obj.file)
            with open(path, 'wb') as f:
                resp.raw.decode_content = True
                f.write(resp.content)
    except Exception as e:
        return e


def _fetch_url(url, local_path_obj):
    """
    :param url: 
    :param dst_folder: 
    :param file_name: 
    :return: (response, exception)

    """
    auth = HTTPBasicAuth(**artifactory_settings['credentials'])

    # url = 'artifactory_prefix/test.tar.gz'
    # dst_folder = "test_tar_dir"

    try:
        resp = requests.get(url, auth=auth)
    except requests.RequestException as e:
        exception = e
        resp = requests.models.Response()
        resp.url = url
    else:
        exception = _put(resp, local_path_obj)

    return resp, exception


def load_artifacts(url_to_folder_mapping_list):
    num_threads = get_num_threads()

    pool = ThreadPool(processes=num_threads)
    responses = pool.map(lambda args: _fetch_url(*args), url_to_folder_mapping_list)
    return responses


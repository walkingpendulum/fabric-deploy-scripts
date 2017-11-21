import logging
import os
import tarfile
from StringIO import StringIO
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as ThreadPool

import requests

from fabric_utils.svc import ArtifactoryTreeHandler as artifactory

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(threadName)s %(levelname)s: %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_num_threads():
    return os.getenv('SERVICE_NUM_THREADS')


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


def _download_uri(uri, local_path_obj, cred_str):
    """
    :param uri: 
    :param dst_folder: 
    :param file_name: 
    :return: (response, exception)

    """
    try:
        resp = artifactory.load(uri, cred_str)
    except requests.RequestException as e:
        exception = e
        resp = requests.models.Response()
        resp.url = uri
    else:
        exception = _put(resp, local_path_obj)
        artifactory.dump_info(uri, local_path_obj, cred_str)
        logger.info("Finish task %s with %s" % (uri, resp))

    return resp, exception


def _handle_task(uri, local_path_obj, cred_str):
    flag = artifactory.check_if_loading_needed(uri, local_path_obj, cred_str)
    if flag:
        task_result = _download_uri(uri, local_path_obj, cred_str)
        return task_result


def load_artifacts(uri_to_folder_mapping_list, cred_str):
    num_threads = get_num_threads()

    pool = ThreadPool(processes=num_threads)
    results = pool.map(lambda args: _handle_task(*args, cred_str=cred_str), uri_to_folder_mapping_list)
    results = filter(None, results)

    return results


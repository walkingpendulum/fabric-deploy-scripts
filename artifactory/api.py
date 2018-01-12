# coding: utf-8
from __future__ import print_function

import sys

from requests.auth import HTTPBasicAuth

from artifactory import downloader
from artifactory.logger import make_logger

info_file = downloader.info_file


def execute_tasks(tasks, cred_str=None):
    """
    
    формат task:
        {
            'url': str,
            'info_url': str,
            'destination': {
                'folder': str,
                'file': str,
            }
        } 
    
    если ключ folder отсутствует, то подставляется abspath('.')
    если ключ info_url отсутствует, то проверки на необходимость загрузки не происходит
    
    :param tasks: 
    :param cred_str: 
    :return: 
    """
    logger = make_logger()
    auth = HTTPBasicAuth(*cred_str.split(':')) if cred_str else None

    results = downloader.execute_download_tasks(tasks, auth)
    fails = filter(lambda x: isinstance(x[1], Exception), results)

    for url, exception in fails:
        logger.error('During url {} donwload exception occurs: {}'.format(url, exception))

    if fails:
        logger.error('Complete with errors, aborting')
        sys.exit(1)

# coding: utf-8
from __future__ import print_function

import datetime
import getpass
import json
import os
import subprocess
import traceback as tb

from fabric.operations import prompt

VAULT_CACERT = os.path.expanduser('vault_ca_cert')
VAULT_ADDR = 'vault_addr'
CA_URL = 'ca_url'
DEFAULT_PATH_PREFIX = 'default_path_prefix'


def _load_cert_if_needed():
    if os.path.exists(VAULT_CACERT):
        return

    os.makedirs(os.path.dirname(VAULT_CACERT))
    subprocess.call('curl -s %s > %s' % (CA_URL, VAULT_CACERT), shell=True)


def _vault_token_lookup():
    output = subprocess.check_output('vault token-lookup -format json', stderr=subprocess.STDOUT, shell=True)
    token_data = json.loads(output)['data']
    return token_data


def _vault_auth():
    default_user = getpass.getuser()
    user = prompt('Authtorization needed, please specify username:', default=default_user)
    subprocess.call('vault auth -method=ldap username=%s' % user, shell=True)


def _check_if_token_needs_attention():
    try:
        token_data = _vault_token_lookup()
        now_str = unicode(datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S'))
        if token_data['expire_time'] < now_str:
            subprocess.call('vault token-renew', shell=True)
    except subprocess.CalledProcessError as exc:
        stderr = exc.output
        if '* missing client token' in stderr:
            _vault_auth()
        else:
            raise


def _check_if_vault_binary_presented():
    try:
        subprocess.check_output('which vault ', shell=True)
        is_installed = True
    except subprocess.CalledProcessError:
        is_installed = False

    if is_installed:
        return

    error_str = "`vault` binary is not located, please install it from https://www.vaultproject.io/downloads.html"
    raise RuntimeError(error_str)


def load_secrets_from_vault(secret_name, path_prefix=None):
    _check_if_vault_binary_presented()

    environment = {'VAULT_CACERT': VAULT_CACERT, 'VAULT_ADDR': VAULT_ADDR}
    os.environ.update(environment)

    _load_cert_if_needed()
    _check_if_token_needs_attention()

    path_prefix = path_prefix or DEFAULT_PATH_PREFIX
    cmd = 'vault read -format json %s' % os.path.join(path_prefix, '%s.json' % secret_name)

    try:
        output = subprocess.check_output(cmd, shell=True)
        secret = json.loads(json.loads(output)['data']['data'])
    except Exception:
        print(tb.format_exc())
        secret = {}

    return secret

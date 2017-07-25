from __future__ import print_function

import json
import os
import sys
import traceback as tb

from default_settings import secrets_path
from fabric_utils.vault import load_secrets_from_vault


def load(secret_name, path=None):
    path = path or secrets_path
    file_path = os.path.join(path, '%s.json' % secret_name)
    try:
        with open(file_path) as f:
            secrets = json.load(f)
    except IOError:
        secrets = load_secrets_from_vault(secret_name=secret_name)
    except Exception:
        print(tb.format_exc())
        secrets = {}

    return secrets


if __name__ == '__main__':
    print(
        json.dumps(
            load(sys.argv[1]),
            indent=4,
            ensure_ascii=False
        )
    )

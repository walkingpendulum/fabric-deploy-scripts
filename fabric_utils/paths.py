import os

GIT_ROOT = os.getenv('GIT_ROOT', os.path.abspath('..'))
ARTIFACTORY_MODEL_TAGS_TABLE_PATH = os.path.join(GIT_ROOT, 'artifactory_model_tags.yml')
DATA_PATH = os.path.join(GIT_ROOT, 'data')

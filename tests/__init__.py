import os
import logging

os.environ['AWS_REGION'] = 'us-west-2'
os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'
os.environ['APP_ENV'] = 'unittest'
logging.basicConfig(level=logging.DEBUG)

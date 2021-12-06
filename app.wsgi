# this file used with Apache webserver

import os
import sys
sys.path.insert(0, '/var/www/flaskr') # required for from flaskr import

# from myflaskrsecrets import secret_key
# https://modwsgi.readthedocs.io/en/master/user-guides/virtual-environments.html
activate_this = '/var/www/.virtualenvs/flaskr-n7hAE5T6/bin/activate_this.py'

with open(activate_this) as file_:
   exec(file_.read(), dict(__file__=activate_this))

from flaskr import create_app
application = create_app()

# application.secret_key = os.getenv('SECRET_KEY', secret_key)
# retrieved from Apache environment, set via /etc/apache2/envvars
#  redundant from __init__.py, but ovewrites with same value, from env 

# application.secret_key = 'for dev'
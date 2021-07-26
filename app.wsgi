import os
import sys
sys.path.insert(0, '/var/www/flaskr')

activate_this = '/var/www/.virtualenvs/flaskr-n7hAE5T6/bin/activate_this.py'

with open(activate_this) as file_:
    exec(file_.read(), dict(__file__=activate_this))

from flaskr import create_app
application = create_app()
# application.secret_key = os.getenv('SECRET_KEY', 'for dev')
# application.config.from_object('flaskr.default_settings')
application.secret_key = os.getenv('SECRET_KEY', 'for dev')
# application.config.from_object('.env')
application.config.from_pyfile('.env')

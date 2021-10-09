from flask import Flask
import os

# import sys
# sys.path.insert(0, '/var/www/flaskr')

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    # app.config.from_mapping(
    #     # SECRET_KEY='dev', via .env?
    #     DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    # )
    app.config.from_mapping(
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # a simple page that says hello
    @app.route('/hello')
    @app.route('/hello/<string:name>')
    def hello(name="world"):
        a = os.getenv('BLAH', 'nutin honey')
        # a={}'.format(a)
        # application.secret_key = os.getenv('SECRET_KEY', 'for dev')
        # import subprocess
        # a = subprocess.run(['env'], stdout=subprocess.PIPE)
        # a.stdout
        return '<h1>Hello there, {}!</h1> from flaskr/__init__.py'.format(name)
        # return '<h1>Hello there, {}!</h1>'.format(app.secret_key)

    @app.route('/hello2')
    @app.route('/about')
    def about():
        return '<h1>About, World!</h1>'

    @app.route('/onlyget', methods=['GET']) # GET POST
    def onlyget():
        return 'You can only GET this webpage.'


    from . import db
    db.init_app(app)

    from . import auth
    app.register_blueprint(auth.bp)

    from . import blog
    app.register_blueprint(blog.bp)

    from . import inventory
    app.register_blueprint(inventory.bp)

    from . import home
    app.register_blueprint(home.bp)
    app.add_url_rule('/', endpoint='index') # needed?

    return app

if __name__ == "__main__":
    app.run(debug=True)

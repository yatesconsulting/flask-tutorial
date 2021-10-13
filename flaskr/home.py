from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)
from werkzeug.exceptions import abort

from flaskr.auth import login_required
from flaskr.db import get_db

# https://github.com/pallets/flask/tree/main/examples/tutorial

bp = Blueprint('home', __name__, url_prefix='/')

@bp.route('/')
def index():
    """Show a menu, with the secrets hidden until logged in."""
    links = ['Blog','Inventory','Duplicate_Cleanup']
    return render_template('home/index.html', links=links)

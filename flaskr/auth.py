import functools

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from werkzeug.security import check_password_hash, generate_password_hash

from flaskr.db import get_db

from flaskr.risd_auth import ADAuthenticated
from myflaskrsecrets import ldapdomain
import re

bp = Blueprint('auth', __name__, url_prefix='/auth')

@bp.route('/register', methods=('GET', 'POST'))
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        db = get_db()
        error = None

        if not username:
            error = 'Username is required.'
        elif not password:
            error = 'Password is required.'
        elif db.execute(
            'SELECT id FROM user WHERE username = ?', (username,)
        ).fetchone() is not None:
            error = f"User {username} is already registered."

        if error is None:
            db.execute(
                'INSERT INTO user (username, password) VALUES (?, ?)',
                (username, generate_password_hash(password))
            )
            db.commit()
            return redirect(url_for('auth.login'))

        flash(error)

    return render_template('auth/register.html')

@bp.route('/login', methods=('GET', 'POST'))
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        # db = get_db()
        error = None
        
        if username is None:
            error = 'No username.'
        elif password is None:
            error = 'No password.'
        else:
            user = ADAuthenticated(username=username, password=password)
            if user.is_authentic():
                # error = "user auth worked for {}".format(user.username)
                pass
            else:
                error = 'Failed Auth'

            if error is None:
                session.clear()
                username = user.username
                if re.search(r"\\",username):
                    username = username.split('\\')[-1]
                elif re.search("@", username):
                    username = username.split('@')[0]
                session['username'] = username # stored in cookie?  prob need to update to something else, maybe
                return redirect(url_for('index'))

            flash(error)

    return render_template('auth/login.html')

@bp.before_app_request
def load_logged_in_user():
    username = session.get('username')

    if username is None:
        g.user = None
    else:
        g.user = {'username':username}
        
        # get_db().execute(
        #     'SELECT * FROM user WHERE id = ?', (user_id,)
        # ).fetchone()

@bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

def login_required(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if g.user is None:
            return redirect(url_for('auth.login'))

        return view(**kwargs)

    return wrapped_view
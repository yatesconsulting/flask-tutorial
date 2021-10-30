from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)
from werkzeug.exceptions import abort

from flaskr.auth import login_required
from flaskr.db import get_db

from . import mssql_db as dbsource
# from . import pyodbc_db as dbsource

bp = Blueprint('inventory', __name__, url_prefix='/inv')

@bp.route('/')
def index():
    """Show the menu"""
    return render_template('inventory/index.html')

@bp.route('/del', methods=["GET", "POST"])
@login_required
def deletions():
    db = dbsource.MSSQL_DB_Conn() # breaks instantly from Apache, but not :5000?? with pyodbc
    # pyodbc commands work from sftp to iveesql3 but not to older sql2012
    # sql = "select * from web..vw_web_FA_base where TagNumber = '{}'".format(57749)
    sql = "select top 10 'blah' as blah, * from web..vw_web_FA_base"
    r = db.execute_s(sql)
    # r=['test234']
    # db.commit()
    return render_template('inventory/index.html',rows=r)

    if request.method == 'POST':
        rows = []
        # data = request.get_data()
        # data = request.stream.read()
        # data = request.form
        # rows.append(data)
        scanned = request.form.getlist("scanned")
        rows.append(request.form)
        # return render_template('inventory/index.html',rows=['blah',rows] )

        cancel = request.form.get('cancel') # if key might not exist
        if cancel:
            return render_template('inventory/index.html',rows=['Cancelled'] )
        error = None

        if not scanned and not cancel:
            error = 'You did not scan anything.'

        if error is not None:
            flash(error)

        else:
            db = dbsource.MSSQL_DB_Conn()
            r = db.execute_s('select * from web..vw_web_FA_base where TagNumber in (?)',(57749))
            # db.commit()
            return render_template('inventory/index.html',rows=r)

    # if FORM
    # db = mssql_db.MSSQL_DB_Conn()

    # ok til here, but next line crashes it
    # a = db.test_results()
    # b = a.__dict__
    # db.spid
    # c = json.dumps(a)

    # a = ['blue','red']
    # sql = "select top 3 *,name,type from web.sys.tables"
    # sql = "select top 10 'blah' as blah, * from web..vw_web_FA_base"
    # r = db.execute_s(sql)
    # return '<code>..{}</code>'.format(r)
    return render_template('inventory/scan.html',pgtitle = "Add to Deleted inventory form")

import io
from datetime import datetime
from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for, send_file, session
)
from werkzeug.exceptions import abort
from flaskr.auth import login_required
from flaskr.db import get_db
from . import pyodbc_db_assessment as dbsource
from flaskr.models.empcal import EmpCal
from flask.helpers import make_response
# import re

dba = dbsource.MSSQL_DB_Conn_assessment()

bp = Blueprint('chromebooks', __name__, url_prefix='/chromebooks')


def _getallchromebooks(dba):
    ''' return a list of school years'''
    sql = f'''select top 10 orgUnitPath, annotatedAssetId as Tag, serialNumber as Serial from Chromebooks where status = 'ACTIVE' '''
    # return sql
    ans = dba.execute_s(sql)
    return ans

def _isvalidschool(school):
    school = school.upper()
    if ",{},".format(school) in ",BES,BMS,MVM,DNE,MMS,EGP,ECE,NLE,MHE,MAE,MON,SUN,SMS,PES,PEC,GHS,UHS,ECHS,RHS,VVE,WAE,SGMS,":
        return True
    return False

def schools():
    return [{'initials':'BES'}, {'initials':'BMS'}, {'initials':'MVM'}, {'initials':'DNE'}, {'initials':'MMS'}, {'initials':'EGP'}, {'initials':'ECE'},{'initials':'NLE'}, {'initials':'MHE'}, {'initials':'MAE'}, {'initials':'MON'}, {'initials':'SGMS'}, {'initials':'SUN'}, {'initials':'SMS'},{'initials':'PEC'}, {'initials':'GHS'}, {'initials':'UHS'}, {'initials':'ECHS'}, {'initials':'RHS'}, {'initials':'VVE'}, {'initials':'WAE'}]

def ous(dba):
    '''return distinct list of all OUs in the Chromebook list'''
    sql = "SELECT distinct orgUnitPath FROM CHROMEBOOKS order by orgUnitPath"
    ans = dba.execute_s(sql)
    a2 = []
    for a in range(len(ans)):
        a2.append({'index':a, 'fullname':ans[a]['orgUnitPath']})
    return a2

@bp.route('/', methods=('GET','POST'))
@login_required
def index():
    """show everything"""

    cbooks = _getallchromebooks(dba) # limit to list requests only
    cbookheaders = ['orgUnitPath', 'Tag', 'Serial']
    return render_template('chromebooks/index.html', cbooks=cbooks, cbookheaders=cbookheaders, schools=schools(), ous=ous(dba) )


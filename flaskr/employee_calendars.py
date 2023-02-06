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

bp = Blueprint('employee_calendars', __name__, url_prefix='/empcal')

outputs = ['year', 'ical'] # 'pdf'
# if admin: outputs.append('edit')


@bp.route('/', methods=('GET','POST'))
def index():
    """Show the menu"""
    yrlist = _getlistofcalyears(dba)
    sample = [{'words':'test 1', 'link':1}, {'words':'test 2', 'link':2}]
    # return render_template('empcals/index.html', menu=yrlist, outputs=outputs)
    if len(yrlist) > 1:
        return render_template('empcals/index.html', menu=yrlist, outputs=outputs)
    else:
        sy = yrlist[0]['link']
        callist = _getlistofcals(dba, sy)
        return render_template('empcals/index.html', menu=callist, outputs=outputs)

@bp.route('/cal/<string:calname>/<string:output>', methods=('GET', 'POST'))
@bp.route('/cal/<string:calname>', defaults={'output':'year'}, methods=('GET', 'POST'))
def cal(calname, output):
    """Show the employee calendar number calname"""

    # year selected, give a selection set
    if '-' in calname:
        sy = _cksyvalid(dba, calname)
        # actually just a year selected, give some more options
        callist = _getlistofcals(dba, sy)
        # dblck = _cksyvalid(dba, sy)
        sample = [{'words':'test 3', 'link':1}, {'words':'test 2', 'link':2}, {'words':callist, 'link':3}]
        return render_template('empcals/index.html', menu=callist, outputs=outputs)

    # if method GET/POST of ical output = 
    
    # an actual calendar and year selected, show calendar
    else:
        cal = EmpCal(calname)
        
        if output == 'ical':
            response = make_response(render_template('empcals/ical.txt', events=cal.raw))
            response.headers.set('Content-Type', 'text/html')
            response.headers.set('Content-Disposition', 'attachment', filename=f'{cal.title}{cal.schoolyear}.ics')
            return response
            # return render_template('empcals/ical.txt', events=cal.raw)
        elif output == 'year':
            return render_template('empcals/month.html', cal=cal)
        else:
            pass # default?

@bp.route('/csv', methods=('GET', 'POST'))
def csv():
    """Create a CSV for full download of the current year for import into web calendar"""
    # required fields:
    # Event,Start Date,End Date,Is All Day,Start Time,End Time,Location,Category,Description,Price,Currency,Contact,Event Registration,Member Participants,Nonmember Participants,Payment Provider
    # Event exs: Twelve Month 242 days Start of contract, Spring Break - Elementary Assistant Principals
    # Start = End ex: 12/23/2022
    # Is All Day = TRUE
    # all others blank
    
    cal = EmpCal("all")
    ans = cal.datesholidsaywithfor()
    # print(ans)
    # print(len(ans))

    response = make_response(render_template('empcals/forimport.csv', events=ans))
    response.headers.set('Content-Type', 'text/html')
    response.headers.set('Content-Disposition', 'attachment', filename=f'AllCalendar{cal.schoolyear}.csv')
    return response

def _getlistofcalyears(dba):
    ''' return a list of school years, for who? - maybe change for admin use in future'''
    sql = f'''select distinct 
    'School year ' + schoolyear as words
    , schoolyear as link
    from EmpCalendars where status = 'active' order by 1 desc'''
    # return sql
    ans = dba.execute_s(sql)
    return ans

def _cksyvalid(dba, sy):
    allyrs = _getlistofcalyears(dba)
    if sy not in [a['link'] for a in allyrs]:
        sy = allyrs[0]['link'] # maybe fix here when "drafts" are defined
    return sy

def _getlistofcals(dba, sy):
    ''' return a list of calendars that can be used for detail views'''
    mysy = _cksyvalid(dba, sy) # sanitize sy to prevent misuse
    sql = f'''select distinct
        id as link
        , isnull(Description,'No Description') + ' (' + cast(isnull(WorkDays,0) as varchar(10)) +  ' days)' as words
        from EmpCalendars
        where schoolyear = '{mysy}'
        '''
    ans = dba.execute_s(sql)
    # return sql
    return ans

def _getlistofcalsforcsvexport(dba):
    ''' return a list of calendars for all csv export'''
    sql = f'''select distinct
        id
        from EmpCalendars
        where status = 'active' and includeincsvexport = 1
        '''
    ans = dba.execute_s(sql)
    # return sql
    return ans
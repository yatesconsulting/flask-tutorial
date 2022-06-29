from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)
from werkzeug.exceptions import abort
# import sys
# sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial/flaskr') # required for from flaskr.models.... import
# sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial') # required for ?import?

# from flaskr import db

from flaskr.auth import login_required
# from flaskr.db import get_db
# from flaskr import pyodbc_db
# from myflaskrsecrets import dbname ## mcn_connet for us
from flaskr.models.dupsets import Dupset
from flaskr.models.dbsetup import Dbsetup
from flaskr.models.SSNTransposedChecker import SSNTransposedCheck

# jdbname = "TmsEPly"
# tbldid = '{}..BAY_DupIDs'.format(dbname)
# tbldip = '{}..BAY_DupsInProgress'.format(dbname)
# tblxkeys = '{}..BAY_DupExtraKeys'.format(dbname)
# tblmerges = '{}..BAY_dipidMergeSelections'.format(dbname)
# tblPKConstraints = '{}..BAY_PKConstraints'.format(dbname)
# db = pyodbc_db.MSSQL_DB_Conn()

bp = Blueprint('duplicate_cleanup', __name__, url_prefix='/dups')    

@bp.route('/')
@login_required
def index():
    """ fill and show [rows] into a simple list of things with a little space between them."""
    rows = []
    rows.append("""You can click on the Duplicate Cleanup header above to return to this page from subpages.""")

    rows.append("""Dups are defined in NameMaster.  If either the BIRTH_NAME or PREFERRED_NAME
     fields start with "dup" or "use", then it is a duplicate.  If either field has an ID in it,
     that is the good ID, and others should be merged into that one. Each set of duplicates has a dupset number.
     Any time you see a list of IDs, the first one is the good ID, all others are bad.""")

    rows.append('/refreshdups refresh the dups (SQL defined in duplicate_cleanup.py)')

    rows.append("""Dup Extra Keys are a list of all the tables that might have records needing merges.
    They are ordered to attempt to streamline updates with minimal errors.
    The base XKEYS of each table are defined here.
    XKEYS are the unique combination of row names that each id_num (or appid) should only have one ID in.""")
    rows.append('/refreshdek refresh the duplicate extra key list (SQL defined in duplicate_cleanup.py)')

    rows.append("""Once these setup steps are done, you can show the list of all dups.
    One goodID may have multiple badIDs in each dupset.
    The ones flagged correctly in NameMaster will say "human verified".
    There is also a check from BIOGRAPH_MASTER showing dup SSN records.""")

    rows.append('/showlist Show all dup sets')

    rows.append("""The details of one dupset will show all tables that require attention.
    Merging is the choosing of which info should be retained on records that can only have one (per Xkey set).
    This choice is made by web form submission from values of each record for each field.  These are color coded 
    and prefilled the best automation can provide.  
    Merging is the updating the goodid record with the info you select, then deleting all bad id records.
    Once you submit this page successfully, it will attempt to process all merge actions along with others 
    in Play and output SQL to screen for use in Production.""")
    rows.append('/showdupset/<dupset>')
    
    rows.append('/resetdupset/<dupset> reset DupsInProgress on this id')
    links = ['refreshdups','ShowList','refreshdek','transposedssncheck']
    return render_template('duplicate_cleanup/index.html', rows=rows, links=links)

@bp.route('/refreshdups', methods=('GET', 'POST'))
@login_required
def refreshdups():
    t = Dbsetup()
    success, msg = t.dbrefreshdups()
    flash (msg)
    if success:
        return redirect(url_for('.showlist'))
    else:
        # return redirect(url_for('.index'))
        return render_template('duplicate_cleanup/index.html') # , rows=[msg])



@bp.route('/transposedssncheck')
@login_required
def transposedssncheck():
    t = SSNTransposedCheck()
    return render_template('duplicate_cleanup/index.html', 
            rows=t.results)

@bp.route('/refreshdek', methods=('GET', 'POST'))
@login_required
def refreshdek():
    t = Dbsetup()
    success, msg = t.dbrefreshdek()
    flash (msg)
    if success:
        return redirect(url_for('.showlist'))
    else:
        return render_template('duplicate_cleanup/index.html') # , rows=[]])

@bp.route('/showlist') # , methods=('GET', 'POST'))
@login_required
def showlist():
    t = Dbsetup()
    success, msg, r = t.dbshowlist()
    flash (msg)
    if success:
        return render_template('duplicate_cleanup/showdupsummary.html', rows=r)
    else:
        return redirect(url_for('.refreshdups'))

@bp.route('/showdupset/<int:dupset>', methods=('GET', 'POST'))
@login_required
def showdupset(dupset):
    fm = dict(request.form)
    ans = Dupset(dupset, fm)
    ans.update_formdata_and_status()
    
    # return render_template('duplicate_cleanup/showdupsetdetail.html',
    #     rows=fm.values(), headerinfo=fm.keys(), sqlinfo=[])

    if ans.status == "magictime":
        ans.attempttheupdate()
        return render_template('duplicate_cleanup/index.html', 
            rows=ans.formbodyinfo)
    elif not ans.goodid:
        return render_template('duplicate_cleanup/index.html', 
            rows=ans.formbodyinfo)
    else:
        return render_template('duplicate_cleanup/showdupsetdetail.html',
            rows=ans.formbodyinfo, headerinfo=ans.formheaderinfo, sqlinfo=ans.sqlinfo)

    # return render_template('duplicate_cleanup/index.html', rows=debugrows)
    # return render_template('duplicate_cleanup/showdupsetdetail.html', rows=[thishtml])

@bp.route('/resetdupset/<int:dupset>', methods=('GET', 'POST'))
@login_required
def resetdupset(dupset):
    ans = Dupset(dupset, {}) # request.form)
    ans._resetDupsInProgress()
    flash ("dupset {} reset".format(dupset))
    # return render_template('duplicate_cleanup/index.html', rows=[ans])
    return redirect(url_for('.showdupset', dupset=dupset))
    # return render_template('duplicate_cleanup/showdupsetdetail.html',
    #     rows=ans.formbodyinfo, headerinfo=ans.formheaderinfo, sqlinfo=ans.sqlinfo)

if __name__ == '__main__':
    print("Hi, don't run this file, use the models instead.")
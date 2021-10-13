from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)
from werkzeug.exceptions import abort

from flaskr.auth import login_required
from flaskr.db import get_db
from . import pyodbc_db
from myflaskrsecrets import dbname ## mcn_connet for us

bp = Blueprint('duplicate_cleanup', __name__, url_prefix='/dups')    

def _dupsummary():
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = """select db as [Database],max(updated) as [LastUpdated], count(distinct dupset) as dupsets
, (select count(*) as badids from {}..BAY_DupIDs where goodid = 0)
as HumanVerifiedButUnmatched
from MCN_Connect..BAY_DupIDs
group by db""".format(dbname)
    r = db.execute_s(sql)
    if r:
        return r[0]
    else:
        return "nothing set yet"

def _zzz_dbfromdups():
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = "select distinct db from {}..BAY_DupIDs".format(dbname)
    r = db.execute_s(sql)
    if db.record_count > 1:
        return ''
    elif db.record_count == 1:
        return r[0].db
    else:
        return ''

@bp.route('/')
@login_required
def index():
    """Show all the posts, most recent first."""
    # test_results
    rows = []
    ans = "TmsEPly"
    rows.append('/refreshdups Select Databse and look for dups (currently {})'.format(_dupsummary()))
    rows.append('Show any human verified but unmatched (called out as dups, but only one ID)')
    rows.append('/showlist Show dup sets')
    rows.append('Select Dup set')
    rows.append('/showdupset Show dups in all tables')
    rows.append('Process dup set for merges')
    return render_template('duplicate_cleanup/index.html', rows=rows)

@bp.route('/refreshdups', methods=('GET', 'POST'))
# @login_required
def refreshdups():
    # if request.method == 'GET':
    # form with database selections 
    # # if request.method == 'POST':
    # if database selection valid, do all this, otherwise prompt for it again
    #     dupset = request.form['dupset']
    #     body = request.form['body']

    db = pyodbc_db.MSSQL_DB_Conn()
    # a = ['blue','red']
    jdbname = "TmsEPly" # Prd
    done = []
    sql = "delete from {}..BAY_DupIDs".format(dbname)
    done.append(sql)
    # sql = "select top 20 * from MCN_Connect.sys.tables"
    r = []
    if (db.execute_i_u_d(sql)):
        # this will be reworked into a MCN_Connect...BAY_sp_...TmsEPly
        sql = """
        insert into {}..BAY_DupIDs 
        (id_num, human_verified, goodid, origtablewithdup, dupset, db)
        select
        id_num
        , 1 as human_verified
        , cast( replace(replace(replace(replace (isnull(PREFERRED_NAME,BIRTH_NAME),'duplicate',''),'use ',''),'dup',''),' ','') as int)
        , 'NameMaster' as origtablewithdup
        , DER2.dupset
        , '{}'
        from {}..NameMaster NM 
        left join 
        (select ROW_NUMBER() OVER(ORDER BY GoodID ASC) AS dupset,
        GoodID from (
        select distinct 
        cast( replace(replace(replace(replace (isnull(PREFERRED_NAME,BIRTH_NAME),'duplicate',''),'use ',''),'dup',''),' ','') as int)
        as GoodID
        from {}..NameMaster where BIRTH_NAME like 'dup%') as DER1) as DER2
        on DER2.goodid = 
        cast( replace(replace(replace(replace (isnull(PREFERRED_NAME,BIRTH_NAME),'duplicate',''),'use ',''),'dup',''),' ','') as int)
        where nm.BIRTH_NAME like 'dup%' or PREFERRED_NAME like 'dup%'
        """.format(dbname, jdbname, jdbname, jdbname)
    done.append(sql)
    # return render_template('duplicate_cleanup/index.html', rows=[sql])
    if (db.execute_i_u_d(sql)):
        sql = """SELECT t.name AS procedure_name
        FROM mcn_connect.sys.procedures AS t
        where t.name like '%BAY%{}'""".format(jdbname)
        # blah
        r = db.execute_s(sql)
        for a in r:
            proc = a['procedure_name']
            done.append(proc)
            sql = "exec {}".format(proc)
            if not (db.execute_i_u_d(sql)):
                return render_template('duplicate_cleanup/index.html', rows=['some thing failed, sql:\n{}'.format(sql)])
        return render_template('duplicate_cleanup/index.html', rows=['everything worked', done])
    # return '<code>..{}</code>'.format(r)
    return render_template('duplicate_cleanup/index.html', rows=['some thing failed, sql:\n{}'.format(sql)])

@bp.route('/showlist') # , methods=('GET', 'POST'))
@login_required
def showlist():
    # if request.method == 'POST':
    #     dupset = request.form['dupset']
    #     body = request.form['body']
    #     error = None

    #     if not dupset:
    #         error = 'Exactly one set of dups must be selected.'

    #     if error is not None:
    #         flash(error)
    #     else:
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = """select distinct db from MCN_Connect..BAY_DupIDs"""
    r = db.execute_s(sql)
    if db.record_count == 0:
        flash("build the dup list first")
        return redirect(url_for('.refreshdups'))
    elif db.record_count > 1:
        flash("Something is wrong, please rebuild the dup list")
        return redirect(url_for('.refreshdups'))
    else:
        jdbname = r[0]['db']
        flash ("found jdbase={}".format(jdbname))


    sql = """ 
    select dupset,D.id_num,human_verified,goodid,origtablewithdup,db
    ,N.LAST_NAME + ', ' + N.FIRST_NAME as LastFirst
    from {}..BAY_DupIDs D
    join {}..name_master N
    on n.ID_NUM = D.id_num
    union
    select dupset,goodid,human_verified,goodid,origtablewithdup,db
    ,N.LAST_NAME + ', ' + N.FIRST_NAME as LastFirst
    from {}..BAY_DupIDs D
    join {}..name_master N
    on n.ID_NUM = D.goodid
    where isnull(d.goodid, 0)>0""".format(dbname,jdbname,dbname,jdbname)
    r = db.execute_s(sql)

    # return redirect(url_for('dups.index'))
    # dups = [{"id_num": 123,"dupset": 1 },{"id_num": 234,"dupset": 1 },{"id_num": 567,"dupset": 2 }]
    # dups = [jdbname,sql]
    # return "hi: {}".format(dups)
    return render_template('duplicate_cleanup/showdupsummary.html', rows=r)

def _listalltableswithid_numcolumns(playorlive):
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = """
    SELECT t.name AS table_name
    FROM {}.sys.tables AS t
    INNER JOIN {}.sys.columns c
    ON t.OBJECT_ID = c.OBJECT_ID
    WHERE c.name = 'id_num'""".format(playorlive, playorlive)
    # return sql    
    return db.execute_s(sql)

def _idsintable(table, ids):
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = """select distinct id_num from {} where id_num in ({})
    """.format(table, ",".join(map(str, ids)) )
    # return sql
    return db.execute_s(sql)

def _colsfromtable(dbname, table):
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = """
    SELECT lower(c.name) AS column_name
    FROM {}.sys.tables AS t
    INNER JOIN {}.sys.columns c ON t.OBJECT_ID = c.OBJECT_ID
    WHERE t.name = '{}'""".format(dbname, dbname, table)
    r = db.execute_s(sql)
    return [b['column_name'] for b in r]


@bp.route('/showdupset/<int:dupset>', methods=('GET', 'POST'))
@login_required
def showdupset(dupset):
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = """
    select 
    D.id_num	
    ,db, goodid
    from {}..BAY_DupIDs D
    where dupset = {}
    union
    select 
    D.goodid as ID_NUM
    ,db, goodid
    from {}..BAY_DupIDs D
    where dupset = {} and  isnull(goodid,0)>0 
    """.format(dbname, dupset, dbname, dupset)
    dupids = db.execute_s(sql)
    jdbname = dupids[0]['db']
    goodid = dupids[0]['goodid']
    ids = [] # I'm sure there is a better zip/**kwargs/map way of doing this better
    for i in dupids:
        ids.append(i['id_num'])
    numids = len(ids)
    rows = []
    ignorelist = ['approwversion','changeuser','changejob','changetime','user_name','job_time']
    rows.append("comparing {} with the goodid of {}".format(ids, goodid))
    r = _listalltableswithid_numcolumns(jdbname)
    r = [{'table_name':'NameMaster'}]
    for t in r:
        table = t['table_name']
        s = _idsintable("{}..{}".format(jdbname, table), ids)
        if len(s) > 1:
            cols = _colsfromtable(jdbname, table)
            sql = "select '{}' as [table]".format(table)
            sqlp2 = ""
            for n in range(numids):
                for c in cols:
                    sql += ", T{}.{} as [T{}{}]".format(n, c, n, c)
                if n > 0:
                    sqlp2 += "join {}..{} T{} on T{}.id_num = {}".format(jdbname, table, n, n, ids[n])
            sql += " from {}..{} T0 {} where T0.id_num = {}".format(jdbname, table, sqlp2, ids[0])
            # ok, now do something with this terribly ugly SQL
            uglysql = db.execute_s(sql)
            # return render_template('duplicate_cleanup/index.html', rows=uglysql)
            thishtml = ""
            for c in cols:
                if c not in ignorelist: # and a lot more logic coming here, for trumps, etc
                    ckthesevals = []
                    for n in range(numids):
                        ttag = "T{}{}".format(n, c)
                        ckthesevals.append(uglysql[0][ttag])
                    if len(set(ckthesevals)) != 1:
                        thishtml += "Col: {}\n".format(c)
                        goodidmarker = ""
                        for n in range(numids):
                            ttag = "T{}{}".format(n, c)
                            if dupids[n]['id_num'] == goodid:
                                goodidmarker = "--"
                            else:
                                goodidmarker = ""
                            thishtml += "{}{}{}\n".format(goodidmarker, uglysql[0][ttag], goodidmarker)
                        
            rows.append("table {} with a {} count (show premium diff){}".format(table, len(s), thishtml))
        elif len(s) == 1 and goodid != s[0]['id_num']:
            rows.append("table {} with only one (not good) ID, {} (show, but hide all?)".format(table, s[0]['id_num']))
        elif len(s) == 1 and goodid == s[0]['id_num']:
            rows.append("table {} with only the good id {} (no changes, don't display)".format(table, s[0]['id_num']))
        # else:
        #     rows.append("table {} had nutin ({})".format(table, len(r)))
    return render_template('duplicate_cleanup/index.html', rows=rows)
    # return render_template('duplicate_cleanup/showdupsetdetail.html', rows=[thishtml])
    # return render_template('duplicate_cleanup/showdupsetdetail.html', rows=rows)

@bp.route('/test')
@login_required
def test():
    cols = {'appid':'constant','id_num':'constant','last_name':'show','first_name':"show",'user_name':"hide",'birth_name':'trump'}
    qtyt = [0,1]
    ignorelist = ['last_name']
    r = [{}]
    rows = {'table': 'NameMaster', 'T0appid': 3734, 'T0id_num': 4303774, 'T0last_name': 'Foo', 'T0first_name': 'Bob', 'T0middle_name': 'P', 'T0prefix': None, 'T0joint_prefix': None, 'T0suffix': None, 'T0title': None, 'T0name_format': None, 'T0name_sts': None, 'T0name_type': None, 'T0party_type': 'P', 'T0birth_name': None, 'T0preferred_name': None, 'T0nickname': None, 'T0udef_5a_2': None, 'T0udef_10a_1': 'MO1194358 ', 'T0udef_id_1': None, 'T0udef_id_2': None, 'T0udef_dte_1': None, 'T0udef_dte_2': None, 'T0approwversion': b'\x00\x00\x00\x009A\xa59', 'T0user_name': 'BRIANB', 'T0job_name': 'w_edit_candidacy',  'T1appid': 61683, 'T1id_num': 4361915, 'T1last_name': 'Foa', 'T1first_name': 'Robert', 'T1middle_name': 'P', 'T1prefix': None, 'T1joint_prefix': None, 'T1suffix': None, 'T1title': None, 'T1name_format': None, 'T1name_sts': None, 'T1name_type': None, 'T1party_type': 'P', 'T1birth_name': 'DUPLICATE ', 'T1preferred_name': '4303774 ', 'T1nickname': None, 'T1udef_5a_2': None, 'T1udef_10a_1': None, 'T1udef_id_1': None, 'T1udef_id_2': None, 'T1udef_dte_1': None, 'T1udef_dte_2': None, 'T1approwversion': b'\x00\x00\x00\x009B\x88@', 'T1user_name': 'MOE', 'T1job_name': 'w_name_master'}
    # 'T[01]udef_11_2_n_1': Decimal('0.00'), breaks things
    # 'T0job_time': datetime.datetime(2020, 1, 30, 8, 56, 50, 570000), 'T1job_time': datetime.datetime(2019, 7, 17, 20, 10, 58, 97000) also
    ans = "TmsEPly"
    
    return render_template('duplicate_cleanup/index2.html', rows=rows, cols=cols, qtyt=qtyt)


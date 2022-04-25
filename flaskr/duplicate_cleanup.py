from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)
from werkzeug.exceptions import abort
from flaskr import db

from flaskr.auth import login_required
from flaskr.db import get_db
from . import pyodbc_db
from myflaskrsecrets import dbname ## mcn_connet for us
from flaskr.models.dupsets import Dupset

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

@bp.route('/')
@login_required
def index():
    """Show all the posts, most recent first."""
    # test_results
    rows = []
    ans = "TmsEPly"
    rows.append('/refreshdups Select Databse and look for dups (currently {})'.format(_dupsummary()))
    rows.append('Show any human verified but unmatched (called out as dups, but only one ID)')
    rows.append('/showlist Show all dup sets')
    rows.append('/showdupset/<dupset> from showlist pg, pick one dupset, show dups in all applicable tables')
    rows.append('/resetdupset/<dupset> re-create DupsInProgress on this id')
    rows.append('Process dup set for merges')
    links = ['refreshdups','ShowList']
    return render_template('duplicate_cleanup/index.html', rows=rows, links=links)

@bp.route('/refreshdups', methods=('GET', 'POST'))
@login_required
def refreshdups():
    # if request.method == 'GET':
    # form with database selections 
    # # if request.method == 'POST':
    # if database selection valid, do all this, otherwise prompt for it again
    #     dupset = request.form['dupset']
    #     body = request.form['body']

    db = pyodbc_db.MSSQL_DB_Conn()
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
        where t.name like 'BAY_sp_dup%{}'""".format(jdbname)
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
    join {}..namemaster N
    on n.ID_NUM = D.id_num
    union
    select dupset,goodid,human_verified,goodid,origtablewithdup,db
    ,N.LAST_NAME + ', ' + N.FIRST_NAME as LastFirst
    from {}..BAY_DupIDs D
    join {}..namemaster N
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
    WHERE c.name = 'id_num' and t.name not like 'bkp%' """.format(playorlive, playorlive)
    # return sql    
    return db.execute_s(sql)

def _idsintable(jdbname, table, ids, ek=[]):
    # return the id_num count, and any other fields that are required for key2 generation
    db = pyodbc_db.MSSQL_DB_Conn()
    eks = ""
    ekw = ""
    if ek:
        # {k1:v1, k2:v2}
        # eks = k1, k2
        # ekw = "and k1='v1' and k2='v2'"
        for a in ek.keys():
            eks += ", {}".format(a)
            ekw += " and {}='{}'".format(a, ek[a])
    
    sql = """select id_num{}, count(*) as  cnt 
        from {}..{} 
        where id_num in ({}) {}
        group by id_num {}""".format(eks, jdbname, table, ",".join(map(str, ids)), ekw, eks)
    # return sql
    return db.execute_s(sql)

def _extrakeys(jdbname, table, ids):
    # return the id_num count, and any other fields that are required for key2 generation
    db = pyodbc_db.MSSQL_DB_Conn()
    ek = ""
    ans = []
    sql = """select [columnname] as cn from {}..BAY_DupExtraKeys
        where tablename = '{}'
        """.format(dbname, table)
    extrakeys = db.execute_s(sql)
    if extrakeys:
        ek = ', '.join(l['cn'] for l in extrakeys)
        sql = """select distinct {} from {}..{} where id_num in ({})
        """.format(ek, jdbname, table, ",".join(map(str, ids)))
        # return ['ek', sql]
        return db.execute_s(sql)
    # return [sql]
    return []

def _rechecksummarycounts(jdbname, table, ids, ek):
    # return the id_num count, and any other fields that are required for key2 generation
    db = pyodbc_db.MSSQL_DB_Conn()
    andwhere = ""
    xcols = ""
    nms = []
    if ek:
        andwhere = " and {}".format(' and '.join(ek))
        for e in ek:
            nms.append(e.split("="))
        xcols = ", {}".format(', '.join(nms))

    sql = """select id_num{}, count(*) as  cnt 
        from {}..{} 
        where id_num in ({}) {}
        group by id_num {}""".format(xcols, jdbname, table, ",".join(map(str, ids)), andwhere, xcols)
    # return sql
    return db.execute_s(sql)    

def _colsfromtable(dbname, table):
    ''' hopefully ID_NUM is always caps, the others will be actual case delivered from here'''
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = """
    SELECT c.name AS column_name
    FROM {}.sys.tables AS t
    INNER JOIN {}.sys.columns c ON t.OBJECT_ID = c.OBJECT_ID
    WHERE t.name = '{}'""".format(dbname, dbname, table)
    r = db.execute_s(sql)
    return [b['column_name'] for b in r]

def _rowsfromtable(jdbname, table, id_num):
    # , keyset2=""):
    db = pyodbc_db.MSSQL_DB_Conn()
    # if keyset2 > "":
    #     keyset2 = "and {}".format(keyset2)
    sql = """
    select *
    from {}..{}
    where id_num = {}
    """.format(jdbname, table, id_num) # , keyset2)
    # return sql
    return db.execute_s(sql)

def _basicdupsetinfo(dbname, dupset):
    """ returns id_num, db, goodid for each in a given dupset"""
    db = pyodbc_db.MSSQL_DB_Conn()
    # this SQL just determines which IDs are used in each table (0, 1, or more)
    sql = """
    select 
    D.goodid as id_num
    ,db, goodid
    from {}..BAY_DupIDs D
    where dupset = {} and  isnull(goodid,0)>0
    union
    select 
    D.id_num	
    ,db, goodid
    from {}..BAY_DupIDs D
    where dupset = {}    
    """.format(dbname, dupset, dbname, dupset)
    r = db.execute_s(sql)
    return r

def _buildformdetaillines():
    pass

def _checkstatusofdupinprogress(dupset):
    db = pyodbc_db.MSSQL_DB_Conn()
    # this SQL determines if a dupset is being worked on, and ready for final processing
    sql = """select username, dupset, count(*) as cnt
        , max(case isnull(xkeys,'') when 'HELPME' then 1 else 0 end) as notdone
        from {}..BAY_DupsInProgress
        where dupset = {}
        group by  username, dupset
    """.format(dbname, dupset)
    r = db.execute_s(sql)
    return r

def _ignorefields(jdbname, table):
    """return ignored fields in the table, or a constant list for now"""
    return ['approwversion','changeuser','changejob','changetime','user_name','job_time']

# def _prepidsforformselection(jdbname, dupset, dipid, ids, table, extrakeys):
#     """returns 
#     [{'table':'NameMaster','extrakeys':'yr_cde=2019,trm_cde=20',
#     'field':'id_num',
#     'missingkeys':'missingkeys',
#     'class':'auto','disabled':'disabled',
#     'options':[{'selected':'selected','showval':'4363131'},
#         {'showval':'4357237', 'diasbled':'disabled'},
#         {'showval':'4366909'}]
#     },
#     ...
#     table, ids, extrakeys: needed to uniquely identify one result set, of more than one row per ID, then we are seeking keys
#     field: each db field is a row in this result set
#     missingkeys: if table is missing keys, fields will be selectable instead of values
#     class: auto|needinput|same|ignore|lockedauto
#         auto = autoselected result based on trumps or nulls, selection not locked
#         needinput = nothing locked, need user input
#         ignore = in ignore column list, user can change, but may be overwritten, like job user
#         lockedauto = keyfield all options locked, but shown
#     options: of the selected set, expecting exactly one line for each id, unless key seeking
#         selected: selected or not present for ONE line selected value
#         showval: value shown to user
#         formval: value submitted to form
#     """
#     db = pyodbc_db.MSSQL_DB_Conn()
#     ignorefields = _ignorefields(jdbname, table)

#     rowj = []
#     wrk = []
#     ans = []
#     goodid = ids[0]
#     cols = _colsfromtable(jdbname, table) # a little redundant, maybe
#     missinggoodid = False # put one in for ID_NUM but everything else ---, if missing
#     morethanonerowperid = False # look for keys

#     xkeys = ""
#     keylist = []
#     if (extrakeys > ""):
#         for ek in extrakeys:
#             for b in ek:
#                 if type(ek[b]) == int:
#                     keylist.append("{}={}".format(b, ek[b]))
#                 else:
#                     keylist.append("{}='{}'".format(b, ek[b]))
#         xkeys = " and {}".format(' and '.join(keylist))
#         keylist = []

#     sql = """select * from {}..{} where id_num in ({}) {}
#     order by ID_NUM""".format( jdbname, table, ",".join(map(str, ids)), xkeys)
#     rows = db.execute_s(sql) 

#     # return rows

#     # ok, let's sort lots of things out
#     # if there is no goodid, then we need to put the goodid in the id_num first row, but all other first rows will be "---"
#     # need to identify which row is the goodid, and put it(them?) first
#     #  so rebuild entire list in correct display order
#     # if there is more than one row for any id, then we need to only offer key selection
#     rowsordered = []
#     missinggoodid = not(goodid in [l['ID_NUM'] for l in rows])
#     idcounts = {}
#     for i in ids:
#         idcounts[i] = 0
    
#     for r in rows:
#         if r['ID_NUM']:
#             idcounts[r['ID_NUM']] += 1
#             if r['ID_NUM'] == goodid:
#                 rowsordered.append(r)
#             else:
#                 rowsordered.insert(0, r)        
#         else:
#             # nothing should ever make it here
#             pass
#     # if any idcounts > 1
#     morethanonerowperid = max([l for l in idcounts]) > 1
#     # idcounts = {}

#     # not sure if I need this
#     # if missinggoodid:
#     #     rowsordered.append({'ID_NUM':'---'})

#     # # if only one row, then all defaults revolve around it, don't check so many things later
#     # if len(rows) == 1:
#     #     if missinggoodid:
#     #         pass

#     # if no goodid, show a line of ---'s in it's place, what ID gets updated to goodid?


#     # if morethanonerowperid == True
#     #   we should only allow selection of keys to reduce sets down
#     #    so lock down key rows with disabled, but not other rows
#     #    lock down all options rows with disabled
#     #    normal "selection" routines? or just leave everything unchecked 
#     #   still hide same/ignore/auto rows

#     for col in cols:
#         wcol = []
#         styleclass = ""
#         if col in ignorefields:
#             styleclass = "ignore"
#         elif extrakeys and col in extrakeys[0].keys():
#             styleclass = "lockedauto"
#         elif col == "ID_NUM":
#             styleclass = "lockedauto"

#         # compare all the values for this col key:
#         if len(rows) == 1:
#             if missinggoodid:
#                 # make a row with a fake ID_NUM = id[0] to push into this set
#                 if col == "ID_NUM":
#                     # put id[0] on this value, and select it, and formvalue = ???
#                     pass
#             else:
#                 # make a form row with good ID and that's the only row, so, yeah
#                 pass
#         for r in rows:
#             if r[col] != "None":
#                 trowval.append(r[col])

#         # auto = autoselected result based on trumps or nulls, selection not locked
#         # needinput = nothing locked, need user input
#         # compare all the values on this row for style=auto or needinput
#         return([missinggoodid, morethanonerowperid,table,col,rows])


#         for r in range(len(rows)):
#             if rows[col]:
#                 wcol.append(rows[col])

#         # for r in rowsthathavegoodid:
#         #     if rows[col] and rows[col]
#         # for r in range(len(rows)):
#         # if r in rowsthathavegoodid:
#         #     continue

#     return ans

def _insertintodiptable(jdbname, dupset, table, ek=""):
    ''' insert into BAY_DupsInProgress'''
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = """insert into {}..BAY_DupsInProgress
    (dupset, tablename, xkeys, db) VALUES ({},'{}','{}','{}')
    """.format(dbname, dupset, table, ek, jdbname)
    db.execute_i_u_d(sql)

def _allnotdonetablesfordupset(dupset):
    ''' return list of only the tables with any partial keys that are in a started dupset, but HELPME flagged''' 
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = """select * from {}..BAY_DupsInProgress where dupset = {} and xkeys = 'HELPME'
    """.format(dbname, dupset)
    return db.execute_s(sql)

def _allkeyscombosforgooddupset(dupset):
    """ return list of only this dupset from prep table with good set of data, no HELPMEs """
    db = pyodbc_db.MSSQL_DB_Conn()
    sql = """select * from {}..BAY_DupsInProgress where dupset = {}
        """.format(dbname, dupset)
    return db.execute_s(sql)

@bp.route('/showdupset/<int:dupset>', methods=('GET', 'POST'))
@login_required
def showdupset(dupset):
    ans = Dupset(dupset)
    # ans.update_status()
    # ans.update_formdata()
    return render_template('duplicate_cleanup/showdupsetdetail.html',
        rows=ans.formbodyinfo, headerinfo=ans.formheaderinfo, sqlinfo=ans.sqlinfo)
    return render_template('duplicate_cleanup/index.html',
        rows=[ans.sqlinfo, ans.formheaderinfo, ans.formbodyinfo])
    
    return render_template('duplicate_cleanup/index.html', rows=debugrows)
    # return render_template('duplicate_cleanup/showdupsetdetail.html', rows=[thishtml])


@bp.route('/resetdupset/<int:dupset>', methods=('GET', 'POST'))
@login_required
def resetdupset(dupset):
    ans = Dupset(dupset)
    # ans.update_status()
    # ans.update_formdata()
    ans._resetDupsInProgress()
    return redirect(url_for('.showdupset', dupset=dupset))
    # return render_template('duplicate_cleanup/showdupsetdetail.html',
    #     rows=ans.formbodyinfo, headerinfo=ans.formheaderinfo, sqlinfo=ans.sqlinfo)


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


@bp.route('/test2')
@login_required
def test2():
    #  guts json dump of all of them
    #################### HERE"S THE KEY
    sqlinfo = {'guts':"{234:['field3=T1.field3','duh=T2.duh']}",
        234:"ffffrom NAME_TYPE_TABLE T0  join NAME_TYPE_TABLE T1 on T1.ID_NUM = 4357237 and blah=blah,s=s join NAME_TYPE_TABLE T2 on T2.ID_NUM = 4366909 and blah=blah,s=s where T0.ID_NUM = 436313 and blah=blah,s=s1<br />delete from NM where ID_NUM = 4357237 and blah=blah,s=s1<br />delete from NM where ID_NUM = 4366909 and blah=blah,s=s1"}
    headerinfo = {'dupset':29, 'goodid':4363131 , 'ids':[4363131, 4357237, 4366909]}
    rowsj = [{'table':'NAME_TYPE_TABLE','field':'id_num','class':'key',
    'xkeys':'blah=blah,s=s','dipid':234,
    'options':[{'selected':'selected','showval':'4363131','disabled':'disabled'},
    {'showval':'4357237','disabled':'disabled'},
    {'showval':'4366909','disabled':'disabled'}]},
    {'table':'NAME_TYPE_TABLE',
    'xkeys':'blah=blah','dipid':234,
    'field':'field2','class':'auto',
    'options':[{'selected':'selected','showval':'B'},
    {'showval':'NULL','formval':'T1'},
    {'showval':'None','formval':'T2'}]},
    {'table':'NAME_TYPE_TABLE',
    'xkeys':'blah=blah','dipid':234,
    'field':'field3','class':'auto',
    'options':[{},
    {'selected':'selected','showval':'C','formval':'T1'},
    {'showval':'None','formval':'T2'}]},
    {'table':'NAME_TYPE_TABLE',
    'xkeys':'blah=blah','dipid':234,
    'field':'field4','class':'ignore',
    'options':[{'selected':'selected','showval':'B'},
    {'showval':'NULL','formval':'T1'},
    {'showval':'None','formval':'T2'}]},
    {'table':'NAME_TYPE_TABLE',
    'xkeys':'blah=blah','dipid':234,
    'field':'field5','class':'same',
    'options':[{'selected':'selected','showval':'B'},
    {'showval':'B','formval':'T1'},
    {'showval':'B','formval':'T2'}]}
    ,
    {'table':'NAME_TYPE_TABLE',
    'xkeys':'blah=blah','dipid':234,
    'field':'field6','class':'needinput',
    'options':[{'showval':'B'},
    {'showval':'C','formval':'T1'},
    {'showval':'D','formval':'T2'}]
    , 'custom':'blah' , 'customdisabled':'disabled' }
    ]
    return render_template('duplicate_cleanup/showdupsetdetail.html', rows=rowsj, headerinfo=headerinfo, sqlinfo=sqlinfo)


@bp.route('/test3')
@login_required
def test3():
    headerinfo = {'dupset':29, 'goodid':4363131 , 'ids':[4363131, 4357237, 4366909]}
    rowsj = [{'table':'NAME_TYPE_TABLE','field':'id_num','class':'auto','disabled':'disabled',
    'xkeys':'blah=blah','choosekeys':'yes','dipid':234,'disabled':'disabled',
    'options':[{'selected':'selected','showval':'4363131'},
    {'showval':'4357237'},
    {'showval':'4366909'}]},
    {'table':'NAME_TYPE_TABLE',
    'xkeys':'blah=blah','choosekeys':'yes','dipid':234,
    'field':'field2','class':'auto',
    'options':[{'selected':'selected','showval':'B','disabled':'disabled'},
    {'showval':'NULL','formval':'T1','disabled':'disabled'},
    {'showval':'None','formval':'T2','disabled':'disabled'}]},
    {'table':'NAME_TYPE_TABLE',
    'xkeys':'blah=blah','choosekeys':'yes','dipid':234,
    'field':'field3','class':'auto',
    'options':[{'disabled':'disabled'},
    {'selected':'selected','showval':'C','formval':'T1','disabled':'disabled'},
    {'showval':'None','formval':'T2','disabled':'disabled'}]},
    {'table':'NAME_TYPE_TABLE',
    'xkeys':'blah=blah','choosekeys':'yes','dipid':234,
    'field':'field4','class':'ignore',
    'options':[{'selected':'selected','showval':'B','disabled':'disabled'},
    {'showval':'NULL','formval':'T1','disabled':'disabled'},
    {'showval':'None','formval':'T2','disabled':'disabled'}]},
    {'table':'NAME_TYPE_TABLE',
    'xkeys':'blah=blah','choosekeys':'yes','dipid':234,
    'field':'field5','class':'same',
    'options':[{'selected':'selected','showval':'B','disabled':'disabled'},
    {'showval':'B','formval':'T1','disabled':'disabled'},
    {'showval':'B','formval':'T2','disabled':'disabled'}]}
    ,
    {'table':'NAME_TYPE_TABLE',
    'xkeys':'blah=blah','choosekeys':'yes','dipid':234,
    'field':'field6','class':'auto','disabled':'disabled',
    'options':[{'showval':'B','disabled':'disabled'},
    {'showval':'C','formval':'T1','disabled':'disabled'},
    {'showval':'D','formval':'T2','disabled':'disabled'}], 'custom':'blah'}

    ]
    return render_template('duplicate_cleanup/showdupsetdetail.html', rows=rowsj, headerinfo=headerinfo)

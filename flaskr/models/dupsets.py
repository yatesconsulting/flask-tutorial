#  this might only be needed when running from the command line, testing
import sys
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial/flaskr') # required for from flaskr.models.... import
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial') # required for ?import?
# from flaskr import pyodbc_db
from flaskr import pyodbc_db
from myflaskrsecrets import dbname ## mcn_connect for us

class Dupset():
    def __init__(self, dupset):
        self.dupset = dupset
        self.error = []
        self.jdbname = ""
        self.dbname = dbname
        self.ids = []
        self.goodid = 0
        self.status = "" # started, needs keys, ready, staged?, complete?
        self.formheaderinfo = {}
        self.formbodyinfo = []
        self.sqlinfo = {}

        self.db = pyodbc_db.MSSQL_DB_Conn() # maybe this is the way to only use one db connection?
        sql = """select distinct db from {}..BAY_DupIDs""".format(dbname)
        r = self.db.execute_s(sql)
        if self.db.record_count > 1:
            self.error.append("Something is wrong, please rebuild the dup list, multiple databases found.")
        else:
            self.jdbname = r[0]['db']
        
        """ id_num, db, goodid for each in a given dupset"""
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
        dupids = self.db.execute_s(sql)
        
        if dupids and not dupids[0]['goodid']:
            self.error.append("Please identify the goodid and refresh duplist first (dupset {})".format(dupset))
        elif dupids:
            self.goodid = dupids[0]['goodid']
            # sort the ids to put goodid first, then ohen others in sorted order, for repeatability later
            self.ids = sorted([l['id_num'] for l in dupids]) # another stackoverflow success story
            self.ids.remove(self.goodid)
            self.ids.insert(0, self.goodid)
        else:
            self.error.append("dupset {} had nothing to look at".format(dupset))
    # setting self.status = started, needs keys, ready, staged?, complete?

    def update_status(self):
        # self.status = not prepped, needs keys, ready, staged?, complete?

        # 1. dupset is inserted into BAY_DupsInProgress noting any that that need more info via HELPME flag in xkey col
        # 2. Work through each HELPME adding new BAY_DupExtraKeys values and trying again
        # 3. all HELME records gone, move on to full merge set of tables
    
        # this SQL determines if a dupset is being worked on, and ready for final processing
        sql = """select  dupset, count(*) as cnt
            , max(case isnull(xkeys,'') when 'HELPME' then 1 else 0 end) as notdone
            from {}..BAY_DupsInProgress
            where dupset = {}
            group by dupset
        """.format(self.dbname, self.dupset)
        ckstatus = self.db.execute_s(sql)
        # print("ckstatus={}".format(ckstatus))

        if ckstatus and ckstatus[0]['notdone']:
            self.status = "needs keys"
        elif ckstatus:
            self.status = "ready"
        else:
            self.status = "not prepped"
        return self.status
    
    def _listalltableswithid_numcolumns(self):
        # db = pyodbc_db.MSSQL_DB_Conn()
        sql = "SELECT * from {}..BAY_DupExtraKeys".format(self.dbname)
        return self.db.execute_s(sql)

    def _idsintable(self, table, ek=[]):
        # return the id_num count, and any other fields that are required for key2 generation
        # db = pyodbc_db.MSSQL_DB_Conn()
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
            group by id_num {}""".format(eks, self.jdbname, table, ",".join(map(str, self.ids)), ekw, eks)
        # return sql
        return self.db.execute_s(sql)

    def _extrakeys(self, table):
        # return the id_num count, and any other fields that are required for key2 generation
        ek = ""
        r = []
        sql = """select xkeys from {}..BAY_DupExtraKeys
            where tablename = '{}'
            """.format(self.dbname, table)
        extrakeys = self.db.execute_s(sql)
        if extrakeys and extrakeys[0]['xkeys']:
            sql = """select distinct {} from {}..{} where id_num in ({})
            """.format(extrakeys[0]['xkeys'], self.jdbname, table, ",".join(map(str, self.ids)))
            r = self.db.execute_s(sql)
            return r
        return []

    def _rechecksummarycounts(self, table, ek):
        # return the id_num count, and any other fields that are required for key2 generation
        # db = pyodbc_db.MSSQL_DB_Conn()
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
            group by id_num {}""".format(xcols, self.jdbname, table, ",".join(map(str, self.ids)), andwhere, xcols)
        # return sql
        return self.db.execute_s(sql)    

    def _colsfromtable(self, table):
        ''' hopefully ID_NUM is always caps, the others will be actual case delivered from here'''
        # db = pyodbc_db.MSSQL_DB_Conn()
        sql = """
        SELECT c.name AS column_name
        FROM {}.sys.tables AS t
        INNER JOIN {}.sys.columns c ON t.OBJECT_ID = c.OBJECT_ID
        WHERE t.name = '{}'""".format(self.jdbname, self.jdbname, table)
        r = self.db.execute_s(sql)
        # print('sql={} and r={}'.format(sql, r))
        return [b['column_name'] for b in r]

    def _rowsfromtable(self, table, id_num):
        # , keyset2=""):
        # db = pyodbc_db.MSSQL_DB_Conn()
        # if keyset2 > "":
        #     keyset2 = "and {}".format(keyset2)
        sql = """
        select *
        from {}..{}
        where id_num = {}
        """.format(self.jdbname, table, id_num) # , keyset2)
        # return sql
        return self.db.execute_s(sql)

    def _basicdupsetinfo(self):
        """ returns id_num, db, goodid for each in a given dupset"""
        # db = pyodbc_db.MSSQL_DB_Conn()
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
        """.format(self.dbname, self.dupset, self.dbname, self.dupset)
        r = self.db.execute_s(sql)
        return r

    def _ignorefields(self, table):
        """return ignored fields in the table, or a constant list for now"""
        return ['APPROWVERSION','CHANGEUSER','CHANGEJOB','CHANGETIME','USER_NAME','JOB_TIME','LAST_UPDATE','JOB_NAME']

    # def _prepFormSelectionFromDupsInProgress(self, dipid, table, extrakeys):
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
    #     ignorefields = self._ignorefields(table)

    #     rowj = []
    #     wrk = []
    #     ans = []
    #     cols = self._colsfromtable(table) # a little redundant, maybe
    #     missinggoodid = False # put one in for ID_NUM but everything else ---, if missing
    #     morethanonerowperid = False # look for keys

    #     xkeys = ""
    #     keylist = []
    #     if (extrakeys):
    #         for ek in extrakeys:
    #             for b in ek:
    #                 if type(ek[b]) == int:
    #                     keylist.append("{}={}".format(b, ek[b]))
    #                 else:
    #                     keylist.append("{}='{}'".format(b, ek[b]))
    #         xkeys = " and {}".format(' and '.join(keylist))
    #         keylist = []

    #     sql = """select * from {}..{} where id_num in ({}) {}
    #     order by ID_NUM""".format( self.jdbname, table, ",".join(map(str, self.ids)), xkeys)
    #     rows = self.db.execute_s(sql) 

    #     # return rows

    #     # ok, let's sort lots of things out
    #     # if there is no goodid, then we need to put the goodid in the id_num first row, but all other first rows will be "---"
    #     # need to identify which row is the goodid, and put it(them?) first
    #     #  so rebuild entire list in correct display order
    #     # if there is more than one row for any id, then we need to only offer key selection
    #     rowsordered = []
    #     missinggoodid = not(self.goodid in [l['ID_NUM'] for l in rows])
    #     idcounts = {}
    #     for i in self.ids:
    #         idcounts[i] = 0
        
    #     for r in rows:
    #         if r['ID_NUM']:
    #             idcounts[r['ID_NUM']] += 1
    #             if r['ID_NUM'] == self.goodid:
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
    #             styleclass = "auto"
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
    #                 pass
    #                 # trowval.append(r[col])

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

    def _insertintodiptable(self, table, ek=""):
        ''' insert into BAY_DupsInProgress'''
        newek = []
        if ek and type(ek) is dict:
            for e  in ek:
                newek.append("{}={}".format(e, ek[e])) # protect tics and insert them???
            ek = ', '.join(newek) 
            # ex: YR_CDE=2017, TRM_CDE=30
        sql = """insert into {}..BAY_DupsInProgress
        (dupset, tablename, xkeys, db) VALUES ({},'{}','{}','{}')
        """.format(self.dbname, self.dupset, table, ek, self.jdbname)
        self.db.execute_i_u_d(sql)

    def _allnotdonetablesfordupset(self):
        ''' return list of only the tables with any partial keys that are in a started dupset, but HELPME flagged''' 
        # db = pyodbc_db.MSSQL_DB_Conn()
        sql = """select * from {}..BAY_DupsInProgress where dupset = {} 
            and xkeys = 'HELPME'
            """.format(self.dbname, self.dupset)
        return self.db.execute_s(sql)

    def _allkeyscombosforgooddupset(self):
        """ return list of only this dupset from prep table with good set of data, no HELPMEs """
        # remove that HELME someday
        sql = """select * from {}..BAY_DupsInProgress where dupset = {}
            and xkeys not like '%HELPME%'
            """.format(self.dbname, self.dupset)
        return self.db.execute_s(sql)
    
    def _singleIDfromTableAndxKeys(self, id, table, xkeys=""):
        if xkeys:
            newxkeys = []
            if "," in xkeys:
                for a in xkeys.split(','):
                    b,c = a.split("=")
                    newxkeys.append("{}='{}'".format(b,c))
            else:
                b,c = xkeys.split("=")
                newxkeys.append("{}='{}'".format(b,c))
            xkeys = ' and {}'.format(' and '.join(newxkeys))
        sql = """select * from {}..{} where id_num = {} {}
            """.format(self.jdbname, table, id, xkeys)
        r = self.db.execute_s(sql)
        return r

    def _loopoveralltables(self, tablelist):
        # for jinja2, self.formheaderinfo has dupset, goodid, ids, and notes?? one per dupset
        self.formheaderinfo = {'dupset':self.dupset, 'goodid':self.goodid, 'ids':self.ids}  
        idscs = ', '.join([str(element) for element in self.ids])
        gutsdict = {}

        for t in tablelist:
            table = t['tablename']
            cols = self._colsfromtable(table)
            # print("cols = {}".format(cols))
            ignorelist = self._ignorefields(table)
            # print("ignorelist: {}".format(ignorelist))
            dipid = t['ID']
            xkeys = "" # YR_CDE=2020, TRM_CDE=30 form
            xkeysdict = {}
            gutslist = [] # ['field3=T1.field3','f2=T2.f2']
            approwversions =[]
            cids = []
            
            if t['xkeys']:
                xkeys = t['xkeys']
                for t1 in xkeys.split(", "):
                    k,v = t1.split('=')
                    xkeysdict[k] = v
                
            # add a variable to handle this when doing HELPMEs
            # else:
            #     xkeys = self._extrakeys(table)
            
            T = []
            # fv = [""] * len(self.ids) # form values for each line in options
            # lockitdownfornewkeycreationonly = False
            # popuate precheck self.sqlinfo if AAPROWVERSION is present
            for index, id in enumerate(self.ids):
                # one row appended for every id, even empty ones and >1 results(todo)
                thisid = self._singleIDfromTableAndxKeys(id, table, xkeys)
                cids.append(thisid)  #  = cids + thisid
                if thisid and 'APPROWVERSION' in thisid[0]:
                    for t in thisid:
                        approwversions.append(t['APPROWVERSION'])
                if thisid:
                    T.append(thisid[0]['ID_NUM'])
                    if len(thisid) > 1:
                        # lockitdownfornewkeycreationonly = True
                        pass # but do something stern here eventually

            axkeys = ""
            if xkeys:
                axkeys = ' and {}'.format(xkeys)
            if approwversions:
                arvcs = ', '.join([str(int.from_bytes(element, 'big')) for element in approwversions]) 
                gids = ', '.join([str(t) for t in T])
                self.sqlinfo["precheck{}".format(dipid)] = """select '{}' as [table],
                    '{}' as xkeys,
                    '{}' as 'GoodCount',
                    ID_NUM,
                    '{}' as allIDs,
                    case when cast(approwversion as bigint) in ({})
                    then 'GoodAppRowVersion'
                    else 'BADAPPROWVERSION' end
                    as AppRowCk
                    from {}..{} 
                    where ID_NUM in ({}) {}<br /><br />
                """.format(table, xkeys, len(T), gids, arvcs, self.jdbname, table, idscs, axkeys)
            else:
                self.sqlinfo["precheck{}".format(dipid)] = "-- sorry, {} has no approwversion<br /><br />".format(table)

            # FOR TESTING:
            # self.formbodyinfo.append([{'table':table, 'dipid': dipid, 'xkeys': xkeys, 
            #     'cids':'{}'.format( cids)}])
            # if the len cids and ids is different, then we must want to give options to
        
            # prep the correct layout, maybe not needed
            # prep = self._idsintable(table, xkeysdict)
            # [{'id_num': 4359752, 'cnt': 1}, {'id_num': 4368418, 'cnt': 1}]
            # print(max([l['cnt'] for l in s]) )
            # cids
            # [[{'ID_NUM': 4368418, 'SOUNDEX_CDE': None}], [{'ID_NUM': 4359752, 'SOUNDEX_CDE': None}]]

            # 4 categories with len(cids) <= len(ids) and no id multi-line
            # show all len(ids) rows, with --- on any missing ones
            # 1. only bad IDs
            #     T0 is first bad id with values
            #     others are T1... with only value ones being assigned T's
            #     update T0 set T0.ID=goodid ... via disabled custom field
            #     custom fields available elsewhere, unlocked
            # 2. only 1 ID, and it's bad
            #     T0 is the bad id
            #     update T0 set T0.ID_NUM=goodid ... via custom field
            # 3. only 1 ID< and it's good
            #     T0 is goodid, and only line showing
            #     allow custom field lines, otherwise update is empty
            # 4. GoodID and >=1 other IDs
            #     T0 is goodid
            #     others shown, emptys show ---
            #     allow custmo fields
            
            

            tjoins = []
            tdels = []
            for index, item in enumerate(T):
                if index == 0:
                    tjoins.append("from {} T0 ".format(table))
                else:
                    tjoins.append("join {} T{} on T{}.ID_NUM = {} {}".format(table, index, index, item, axkeys))
                    tdels.append("delete from {} where ID_NUM = {} {}<br />".format(table, item, axkeys))
                    # need to triple check this works with one bad id
            tjoins.append("where T0.ID_NUM = {} {}<br />".format(T[0], axkeys))
            self.sqlinfo[dipid] = "{}".format('<br />'.join(tjoins + tdels))
            # print('dipid={}, self.sqlinfo[dipid]={}'.format(dipid, self.sqlinfo[dipid]))

            # print("{} of {}  with max of a single id {} for table {}".format(len(prep),
            #     len(self.ids), max([l['cnt'] for l in prep]), table))

            # self.sqlinfo = {'guts':"{234:['field3=T1.field3','duh=T2.duh']}",
            #         234:"ffffrom NAME_TYPE_TABLE T0  join NAME_TYPE_TABLE T1 on T1.ID_NUM = 4357237 and blah=blah,s=s join NAME_TYPE_TABLE T2 on T2.ID_NUM = 4366909 and blah=blah,s=s where T0.ID_NUM = 436313 and blah=blah,s=s1<br />delete from NM where ID_NUM = 4357237 and blah=blah,s=s1<br />delete from NM where ID_NUM = 4366909 and blah=blah,s=s1"}
            #     headerinfo = {'dupset':29, 'goodid':4363131 , 'ids':[4363131, 4357237, 4366909]}
            #  self.formbodyinfo =
            # [{'table':'NAME_TYPE_TABLE','field':'id_num','class':'key',
            #     'xkeys':'blah=blah,s=s','dipid':234,
            #     'options':[{'selected':'selected','showval':'4363131','disabled':'disabled'},
            #     {'showval':'4357237','disabled':'disabled'},
            #     {'showval':'4366909','disabled':'disabled'}],
            # , 'custom':'blah' , 'customdisabled':'disabled' }
            #  -- for each field append one like this
            for col in cols:
                cl = ""
                options= []
                # option = {}
                myformbodyinfo = {}
                # vals = []
                # val = ""

                myformbodyinfo['table'] = table
                myformbodyinfo['field'] = col
                myformbodyinfo['xkeys'] = xkeys
                myformbodyinfo['dipid'] = dipid

                if col.upper() in ignorelist:
                    cl = 'ignore'
                
                # initialize each option
                for a in cids:
                    for cid in a:
                        options.append({'showval':cid[col]})

                # for normal selections, not keyfinding
                for i in range(1, len(options)):
                    options[i]['formval'] = 'T{}'.format(i)

                v = [o['showval'] for o in options]
                dupck = list(set(v))
                if None in dupck:
                    dupck.remove(None)

                # if ignoring this column, then just put all values in, and default to first one   
                # do the same thing if all values are None
                # if col.upper() == "USER_NAME":
                #     myformbodyinfo['custom'] = 'BAYMerge'
                #     # myformbodyinfo['customdi  sabled'] = 'disabled'
                #     gutslist.append("USER_NAME='{}'".format(myformbodyinfo['custom']))
                #     for i in range(len(options)):
                #         options[i]['disabled'] = 'disabled'


                if cl == 'ignore' or not dupck or (col.upper() == 'ID_NUM' and cids[0]):
                    options[0]['selected'] = 'selected'
                    # print('ignoring {}'.format(col))
                    cl  = 'auto' # fix to key later if needed

                elif col.upper() == 'ID_NUM' and not cids[0]:
                    # default to goodid using the 
                    myformbodyinfo['custom'] = self.goodid
                    gutslist.append("ID_NUM={}".format(self.goodid)) # ticless int
                
                # we need to find out if we can auto pick the selection
                else:
                    if len(dupck) == 1:
                        i = v.index(dupck[0])
                        options[i]['selected'] = 'selected'
                        cl = 'auto'
                        if i > 0:
                            gutslist.append('{}=T{}.{}'.format(col, i, col))
                    else:
                        cl = 'needinput'
                    
                if col in xkeysdict.keys() or col.upper() == 'ID_NUM':
                    # disable all options, but not the select or else the required style gets wonky
                    for i in range(len(options)):
                        options[i]['disabled'] = 'disabled'
                    cl = 'key'
                    myformbodyinfo['customdisabled'] = 'disabled'

                myformbodyinfo['class'] = cl
                myformbodyinfo['options'] = options
                self.formbodyinfo.append(myformbodyinfo)
            # include a guts list for every dipid, even if empty
            gutsdict[dipid] = gutslist
            # print('guts for {} was {}'.format(dipid, gutslist))
        self.sqlinfo['guts'] = str(gutsdict) # probj json, not str "{234:['field3=T1.field3','duh=T2.duh']}"
        # print("hey, myguts = {}".format(self.sqlinfo['guts']))
        # print("self.formheaderinfo={}".format(self.formheaderinfo))
        # print("self.formbodyinfo={}".format(self.formbodyinfo))

        # for jinja2, self.formbodyinfo is the layout of the html form, one per dipid
        # each row is a dictionary with keys: table, dipid, xkeys, field, class (auto|needinput|same|ignore),
        #  diabled(disable|), options [{selected(selected|),disabled(disabled|),showval, formval}]
        #  , custom:(value|), customdisabled (diabled|)


        # is the goodid one of the ids returned?
        # goodidins = self.goodid in [l['id_num'] for l in s]
        # return render_template('duplicate_cleanup/index.html', rows=[goodidins])
            
        # # . multi lines with a good id, and all the others, so show table for sure                        
        # if len(s) > 1 and goodidins and len(s) == len(ids):
        #     debugrows.append("table {} (1 goodid) with a {} count (show premium diff){}".format(table, len(s), ""))
        
        # # . multi lines with a good id, and one or more others, but not all of them
        # elif len(s) > 1 and goodidins:
        #     debugrows.append("table {} (1 goodid) with a {} count, missing one or more dup ids){}".format(table, len(s), ""))

        # # . one lines with a not id, so it just has one update statement and no user intervention
        # # show it only with the key columns that will be updated shown
        # elif len(s) == 1 and not goodidins:
        #     debugrows.append("table {} (0 goodid) 1 ID, {} (show, key update)".format(table, s[0]['id_num']))        
        
        # # . table has only one entry, for our goodid, no updates
        # elif len(s) == 1 and goodidins:
        #     debugrows.append("table {} (1 goodid) only ID, ignore/display?".format(table))        

        # # . nothing to see here, really nothing with those keys in this table
        # elif len(s) == 0:
        #     debugrows.append("table {} is not interesting to us".format(table)) 

        # # . BAD, nothing should get here, all cases should have been handled
        # else:
        #     debugrows.append("table {} had nutin - BAD BAAAD".format(table))
        # #  now that the metadata is gathered, let's do something with each column
    
        

            
            # must pull table summaries first and loop over them, this takes forever without
            # r=[]
            # rowcountsok = True
            # for id in ids:
            #     ans = _rowsfromtable(jdbname, table, id)
            #     # return render_template('duplicate_cleanup/index.html', rows=[ans2])
            #     if len(ans) > 1:
            #         # table isinvalid, not enough keys probably
            #         rowcountsok = False
            #     r.append(ans)
            # if rowcountsok and r:
            #     # look it over, maybe missing goodids, or all records, etc
            #     debugrows.append("processing {} with id count of {}".format(table, len(r)))
            # # else:
            #     # return render_template('duplicate_cleanup/index.html', rows=['noteallyok',r])

            

            # sql = "select '{}' as [table]".format(table)
            # sqlp2 = ""
            # for n in range(numids):
            #     for c in cols:
            #         sql += ", T{}.{} as [T{}{}]".format(n, c, n, c)
            #     if n > 0:
            #         sqlp2 += "join {}..{} T{} on T{}.id_num = {}".format(jdbname, table, n, n, ids[n])
            # sql += " from {}..{} T0 {} where T0.id_num = {}".format(jdbname, table, sqlp2, ids[0])
            # # ok, now do something with this terribly ugly SQL
            # # return render_template('duplicate_cleanup/index.html', rows=[sql])
            # # uglysql = db.execute_s(sql)
            # # return render_template('duplicate_cleanup/index.html', rows=uglysql)
            # for c in cols:
            #     if c in ignorelist:
            #         # forj needs line for each id, just select the first one since all form fields are required
            #         onerow = []
            #         onerow.append(jdbname)
            #         onerow.append(c)
            #         for n in range(numids):
            #             for c in cols:
            #                 "T{}{}".format(n, c)
            #         rowsj.append(onerow)

            #     else:
            #         ckthesevals = []
            #         for n in range(numids):
            #             ttag = "T{}{}".format(n, c)
            #             # ckthesevals.append(uglysql[0][ttag])
            #         if len(set(ckthesevals)) != 1:
            #             # thishtml += "Col: {}\n".format(c)
            #             goodidmarker = ""
            #             for n in range(numids):
            #                 ttag = "T{}{}".format(n, c)
            #                 if dupids[n]['id_num'] == goodid:
            #                     goodidmarker = "--"
            #                 else:
            #                     goodidmarker = ""
            #                 # thishtml += "{}{}{}\n".format(goodidmarker, uglysql[0][ttag], goodidmarker)


            # rows = self._prepFormSelectionFromDupsInProgress(dipid, table, extrakeys)???



    # @bp.route('/showdupset/<int:dupset>', methods=('GET', 'POST'))
    # @login_required
    def update_formdata(self):
        numids = len(self.ids)
        jcols = []
        tablelist = []

        # 1. dupset is inserted into BAY_DupsInProgress noting any that that need more info via HELPME flag in xkey col
        # 2. Work through each HELPME adding new BAY_DupExtraKeys values and trying again
        # 3. all HELME records gone, move on to full merge set of tables

        # initialize self.status if not set: "needs keys" |  "ready" | "not prepped"
        if not self.status:
            self.update_status()

        if self.status == "not prepped":
            # nothing for this dupset, let's fill the BAY_DupsInProgress best we can
            # then check it again
            tablelist = self._listalltableswithid_numcolumns()
            # tablelist = [{'tablename':'TRANSCRIPT_HEADER'}]
            for t in tablelist:
                table = t['tablename']
                cols = self._colsfromtable(table)
                extrakeys = self._extrakeys(table)
                # extrakeys = [{'YR':2017, 'TRM':10}, {'YR':2017	, 'TRM':20}, {'YR':2017	, 'TRM':30}]
                helpme = ""
                if extrakeys:
                    for ek in extrakeys:
                        s = self._idsintable(table, ek)
                        if s and max([l['cnt'] for l in s]) > 1:
                            helpme = 'HELPME'                            
                        # _buildformdetaillines(jdbname, table, s, ids, cols, ek)
                    if s and helpme:
                        self._insertintodiptable(table, helpme)
                    elif s:
                        for ek in extrakeys:
                            self._insertintodiptable(table, ek) # store ek as dictionary
                else:
                    s = self._idsintable(table)
                    helpme = ""
                    if s and max([l['cnt'] for l in s]) > 1:
                        helpme = "HELPME"
                    if s:
                        self._insertintodiptable(table, helpme)
            self.update_status()

        # now self.status should be either "needs keys" or  "ready
        if self.status == "needs keys":
            # not all ready, define some more keys
            # just work with the notdone flagged ones
            # tablelist = self._allnotdonetablesfordupset() 
            # finish this here for HELPME flagged ones, probably filling default xkeys on this query
            
            # stub for now, just drop HELPME  rows
            tablelist = self._allkeyscombosforgooddupset()
            self._loopoveralltables(tablelist)

        elif self.status == "ready":
            # must all be OK, let's do it for real
            tablelist = self._allkeyscombosforgooddupset()
            self._loopoveralltables(tablelist)
        else:
            # very odd
            print("error 534, eject, something is very wrong")

        # ok, self.formbodyinfo should be filled now, return anything?

###########################
#the below is a manual test.
if __name__ == '__main__':
    dupset = 83
    sumpin = Dupset(dupset)
    # print(sumpin.error)
    # print(sumpin.ids)
    # print(sumpin.dupset)
    # print(sumpin.jdbname)
    # print(sumpin.goodid)
    # print(sumpin.status)
    # print(sumpin.update_status())
    # sumpin.build_table_list()
    # print(sumpin.status)
    sumpin.update_formdata()
    # print(sumpin._idsintable('NameMaster'))
    # [{'id_num': 4359752, 'cnt': 1}, {'id_num': 4368418, 'cnt': 1}]
    # print(max([l['cnt'] for l in s]) )

    # print(sumpin.formheaderinfo)
    # print(sumpin.formbodyinfo)

    # print(sumpin._listalltableswithid_numcolumns())
    # # print(sumpin._idsintable('NameMaster', ek=[]))
    # print(sumpin._idsintable('NameMaster'))
    # print(sumpin._extrakeys('NameMaster'))
    # # print(sumpin._rechecksummarycounts('NameMaster', ek))
    # print(sumpin._colsfromtable('NameMaster'))
    # # print(sumpin._rowsfromtable('NameMaster', id_num))
    # print(sumpin._basicdupsetinfo())
    # print(sumpin._checkstatusofdupinprogress())
    # print(sumpin._ignorefields('NameMaster'))
    # # print(sumpin._prepFormSelectionFromDupsInProgress(dipid, table, extrakeys))
    # # print(sumpin._insertintodiptable(table, ek=""))
    # print(sumpin._insertintodiptable('NameMaster'))
    # print(sumpin._allnotdonetablesfordupset())
    # print(sumpin._allkeyscombosforgooddupset())

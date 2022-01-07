#  this might only be needed when running from the command line, testing
from os import replace
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
        self.appids = []
        self.goodid = 0
        self.status = "" # started, needs keys, ready, staged?, complete?
        self.formheaderinfo = {}
        self.formbodyinfo = []
        self.sqlinfo = {}
        self._table = ""
        self._id_num = ""

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
            self.ids = sorted([l['id_num'] for l in dupids]) # 'id_num' req'd here
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
        # ID	tablename	xkeys	ID_NUM
        # 827	ITEMS	GROUP_NUMBER, ACTION_CODE	ID_NUMBER
        sql = "SELECT * from {}..BAY_DupExtraKeys order by id".format(self.dbname)
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
                if ek[a]:
                    ekw += " and {}='{}'".format(a, ek[a])
                else:
                    ekw += " and isnull({},'')=''".format(a)
        
        sql = """select {}{}, count(*) as  cnt 
            from {}..{} 
            where {} in ({}) {} group by {} {}""".format(self._id_num, eks,
            self.jdbname, table, self._id_num, ",".join(map(str, self.ids)), ekw, self._id_num, eks)
        # return sql
        return self.db.execute_s(sql)

    def _dupextrakeysuniqvaluekeys(self, table):
        # return the id_num count, and any other fields that are required for key2 generation
        r = []
        sql = """select xkeys from {}..BAY_DupExtraKeys
            where tablename = '{}'
            """.format(self.dbname, table)
        extrakeys = self.db.execute_s(sql)
        if extrakeys and extrakeys[0]['xkeys']:
            sql = """select distinct {} from {}..{} where {} in ({})
                """.format(extrakeys[0]['xkeys'], self.jdbname, self._table, self._id_num, 
                ",".join(map(str, self.ids)))
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

        sql = """select {}{}, count(*) as  cnt 
            from {}..{} 
            where {} in ({}) {}
            group by id_num {}""".format(self._id_num, xcols,
            self.jdbname, table, self._id_num,
            ",".join(map(str, self.ids)), andwhere, xcols)
        # return sql
        return self.db.execute_s(sql)    

    def _colsfromtable(self, table):
        """return the column_names for this table"""
        sql = """
        SELECT c.name AS column_name
        FROM {}.sys.tables AS t
        INNER JOIN {}.sys.columns c ON t.OBJECT_ID = c.OBJECT_ID
        WHERE t.name = '{}'""".format(self.jdbname, self.jdbname, self._table)
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

    def _insertintodiptable(self, table, ek=""):
        ''' insert into BAY_DupsInProgress'''
        newek = []
        if ek and type(ek) is dict:
            for e  in ek:
                if ek[e]:
                    newek.append("{}=''{}''".format(e, ek[e])) # protect tics and insert them???
                else:
                    newek.append("isnull({},'''') = ''''".format(e))
            ek = ' and '.join(newek) 
            # ex: YR_CDE='2017' and TRM_CDE='30'
        sql = """insert into {}..BAY_DupsInProgress
        (dupset, tablename, xkeys, db) VALUES ({},'{}','{}','{}')
        """.format(self.dbname, self.dupset, self._table, ek, self.jdbname)
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
        # remove that HELPME removal someday
        sql = """select * from {}..BAY_DupsInProgress where dupset = {}
            and xkeys not like '%HELPME%'
            """.format(self.dbname, self.dupset)
        return self.db.execute_s(sql)
    
    def _singleIDfromTableAndxKeys(self, id, table, xkeys=""):
        axkeys = ""
        if xkeys:
            axkeys = ' and {}'.format(xkeys)
        sql = """select * from {}..{} where {} = {} {}
            """.format(self.jdbname, table, self._id_num, id, axkeys)
        r = self.db.execute_s(sql)
        return r

    def _id_numFortable(self):
        sql = "SELECT ID_NUM from {}..BAY_DupExtraKeys where tablename = '{}'".format(self.dbname, self._table)
        r = self.db.execute_s(sql)
        return r[0]['ID_NUM'] # 'id_num' ok here

    def _loopoveralltables(self, tablelist):
        # for jinja2, self.formheaderinfo has dupset, goodid, ids, and notes?? one per dupset
        self.formheaderinfo = {'dupset':self.dupset, 'goodid':self.goodid, 'ids':self.ids}  
        idscs = ', '.join([str(element) for element in self.ids])
        gutsdict = {}

        for t in tablelist:
            table = t['tablename']
            self._table = t['tablename']
            self._id_num = self._id_numFortable()
            cols = self._colsfromtable(table) # redunant from self.columns ?
            # print("cols = {}".format(cols))
            ignorelist = self._ignorefields(table)
            # print("ignorelist: {}".format(ignorelist))
            dipid = t['ID']
            xkeys = "" # YR_CDE=2020 and TRM_CDE=30 form
            # xkeysdict = {}
            gutslist = [] # ['field3=T1.field3','f2=T2.f2']
            if table == 'NAME_HISTORY':
                gutslist.append('UDEF_5a_1=DUPFX')
            approwversions =[]
            cids = []
            
            if t['xkeys']:
                xkeys = t['xkeys']
                
            # add a variable to handle this when doing HELPMEs
            # else:
            #     xkeys = self._dupextrakeysuniqvaluekeys(table)
            
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
                    T.append(thisid[0][self._id_num])
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
                    {},
                    '{}' as allIDs,
                    case when cast(approwversion as bigint) in ({})
                    then 'GoodAppRowVersion'
                    else 'BADAPPROWVERSION' end
                    as AppRowCk
                    from {}..{} 
                    where {} in ({}) {}<br /><br />
                """.format(table, xkeys.replace("'","''"), len(T), self._id_num, gids, arvcs, self.jdbname, table, self._id_num, idscs, axkeys)
            else:
                self.sqlinfo["precheck{}".format(dipid)] = "-- sorry, {} has no approwversion<br /><br />".format(table)

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
                    tjoins.append(" from {} T0 ".format(table))
                else:
                    tjoins.append(" join {} T{} on T{}.{} = {} {}".format(table, index, index, self._id_num, item, axkeys))
                    tdels.append("<br>delete from {} where {} = {} {}".format(table, self._id_num, item, axkeys))
                    # need to triple check this works with one bad id
            tjoins.append("where T0.{} = {} {} ".format(self._id_num, T[0], axkeys))
            self.sqlinfo[dipid] = "{}".format(' '.join(tjoins + tdels))

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
                myformbodyinfo = {}

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
                #  DO NOT USE THIS, final update for DUPFX in NAME_MASTER as only mouse trail
                # if col.upper() == "USER_NAME":
                #     myformbodyinfo['custom'] = 'BAYMerge'
                #     # myformbodyinfo['customdi  sabled'] = 'disabled'
                #     gutslist.append("USER_NAME='{}'".format(myformbodyinfo['custom']))
                #     for i in range(len(options)):
                #         options[i]['disabled'] = 'disabled'

                if cl == 'ignore' or not dupck or (col.upper() == self._id_num.upper() and cids[0]):
                    options[0]['selected'] = 'selected'
                    cl  = 'auto' # fix to key later if needed

                elif col.upper() == self._id_num.upper() and not cids[0]:
                    # default to goodid using the 
                    myformbodyinfo['custom'] = self.goodid
                    gutslist.append("{}={}".format(self._id_num, self.goodid)) # ticless int
                
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
                    
                if col.upper() in xkeys.upper() or col.upper() == self._id_num.upper():
                    # disable all options, but not the select or else the required style gets wonky
                    # col in xkeys is a little sloppy, but should be fine, searching field name in SQL
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

    def update_formdata(self):
        numids = len(self.ids)
        jcols = []
        tablelist = []

        # 1. dupset is inserted into BAY_DupsInProgress noting any that that need more info via HELPME flag in xkey col
        # 2. Work through each HELPME adding new BAY_DupExtraKeys values and trying again
        # 3. all HELPME records gone, move on to full merge set of tables

        # initialize self.status if not set: "needs keys" |  "ready" | "not prepped"
        if not self.status:
            self.update_status()

        if self.status == "not prepped":
            # nothing for this dupset, let's fill the BAY_DupsInProgress best we can
            # then check it again
            tablelist = self._listalltableswithid_numcolumns()
            # tablelist = [{'tablename':'TRANSCRIPT_HEADER'}]
            for t in tablelist:
                self._table = table =  t['tablename']
                self._id_num = t['ID_NUM']  # 'id_num' ok here
                # table = t['tablename']
                cols = self._colsfromtable(table) # prob redunant from self.db.columns ?
                extrakeys = self._dupextrakeysuniqvaluekeys(table)
                # extrakeys = [{'YR':2017, 'TRM':10}, {'YR':2017, 'TRM':20}, {'YR':2017, 'TRM':30}]
                helpme = ""
                # try:
                if extrakeys:
                    for ek in extrakeys:
                        s = self._idsintable(table, ek)
                        # print("ek={} s={}".format(ek, s))
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
                # except Exception as e:
                #     pass # skeip tables with errors in xkeys TODO fix
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
            print("error 511, eject, something is very wrong")

        # ok, self.formbodyinfo should be filled now, return anything?

###########################
#the below is a manual test.
if __name__ == '__main__':
    dupset = 84
    sumpin = Dupset(dupset)
    # print(sumpin.error)
    # print(sumpin.ids)
    # print(sumpin.dupset)
    # print(sumpin.jdbname)
    # print(sumpin.goodid)
    # print(sumpin.status)
    # print(sumpin.update_status())
    # sumpin.build_table_list()
    print(sumpin.status)
    sumpin.update_formdata()
    print(sumpin._idsintable('NameMaster'))
    # [{'id_num': 4359752, 'cnt': 1}, {'id_num': 4368418, 'cnt': 1}]
    # print(max([l['cnt'] for l in s]) )

    print(sumpin.formheaderinfo)
    # print(sumpin.formbodyinfo)

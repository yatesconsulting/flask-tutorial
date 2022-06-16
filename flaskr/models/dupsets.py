#  this might only be needed when running from the command line, testing
from os import replace
import string
import sys
import json
import re
# from time import time
from datetime import datetime
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial/flaskr') # required for from flaskr.models.... import
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial') # required for ?import?
from flaskr import pyodbc_db
from myflaskrsecrets import dbname, jdbname, tbldid, tbldip, tblxkeys, tblmerges, tblPKConstraints, tblnamemaster

class Dupset():
    def __init__(self, dupset, requestform):
        self.dupset = dupset # number of the duplicate set of 2 or more IDs to merge
        self.jdbname = jdbname # always run against play, to generate script for use on live after verification
        self.db = pyodbc_db.MSSQL_DB_Conn() # maybe this is the way to only use one db connection?
        self.dbname = dbname
        self.tbldid = tbldid
        self.tbldip = tbldip
        self.tblxkeys = tblxkeys
        self.tblmerges = tblmerges
        self.tblPKConstraints = tblPKConstraints
        self.tblNameMaster = tblnamemaster
        self.requestform = {}
        self.savemergeinfo = {}
        self.ids = {}
        self.goodid = 0
        self.goodappid = 0
        # self.goodgoid = 0
        self.formheaderinfo = {}
        self.formbodyinfo = []
        self.sqlinfo = {}
        self.status = "" # needs keys, pickmerges, magictime
        self.finalsql = []
        self.debug = 1 # 3 will print at start of every def

        self.update_status()
                
        if requestform:
            self.requestform = requestform
            self._processrequestform()
        
        dupids = self._dupsetids() # id_num, goodid for each in a given dupset
        # print('dupids {}'.format(dupids))
        if dupids and not dupids[0]['goodid']:
            self.formbodyinfo.append("ERROR: missing goodid")
            self.formbodyinfo.append("Please identify the goodid and refresh duplist first (dupset {})".format(dupset))
        elif len(dupids) == 1:
            self.formbodyinfo.append("ERROR: goodid is not a valid ID_NUM")
            self.formbodyinfo.append("Please correct the goodid and refresh duplist first (dupset {})".format(dupset))
        elif dupids:
            for i in range(len(dupids)):
                if dupids[i]['id_num'] == dupids[i]['goodid']:
                    self.goodid = dupids[i]['goodid']
                    self.goodappid = dupids[i]['appid']
                    # self.goodgoid = dupids[0]['goid'] # etc
            # sort the ids to put goodid first, then the others in sorted order, for repeatability later
            temp = []
            temp = sorted([l['id_num'] for l in dupids]) # 'id_num' req'd here
            temp.remove(self.goodid)
            temp.insert(0, self.goodid)
            self.ids['id_num'] = temp
            temp = []
            temp = sorted([l['appid'] for l in dupids])
            temp.remove(self.goodappid)
            temp.insert(0, self.goodappid)
            self.ids['appid'] = temp
            # temp = []
            # temp = sorted([l['goid'] for l in dupids])
            # temp.remove(self.goodgoid)
            # temp.insert(0, self.goodgoid)
            # self.ids['goid'] = temp

            # self.update_formdata_and_status() # don't always need this, so don't always do it

        else:
            self.formbodyinfo.append("dupset {} had nothing to look at".format(dupset))

    def _processrequestform(self):
        """insert merge info into BAY_dipidMergeSelections per the form submission"""        
        if self.debug > 3: print("def _processrequestform")
        # or just handle it maybe

        # 58943.FIRST_TIME_POST_SECONDARY=T1
        # 58952.TEL_WEB_GRP_CDE=T1
        # 58952.UDEF_10A_1=T1
        # 68349.NICKNAME:Other
        # 68349.NICKNAME.custom=L'eroy

        # ID	tablename	xkeys
        # 58816	ORG_TRACKING	{"ORG_ID_NUM": 4308976}
        # 58824	AlternateContactMethod	{"ADDR_CDE": "*EML"}
        
        dipids = []
        insertdict = {} # fix me for tic protection/storage, somehow TODO
        for key in self.requestform:
            appendme = ""
            dipid,fld = key.split('.', 1)
            v = self.requestform[key]
            if dipid not in dipids:
                dipids.append(dipid)
                insertdict[dipid] = []
            if v and v[0] == "T" and len(v) > 1 and v[1:].isdigit():
                appendme = "{fld}={}.{fld}".format(v, fld=fld)
                insertdict[dipid].append(appendme)
            elif v and v != "Other":
                v = v.replace("'","") # Cheap sql injecction fix, is it OK? Enough?
                fld = fld.replace('.custom','')
                appendme = "{}='{}'".format(fld, v)
                insertdict[dipid].append(appendme)
                # "MIDDLE_NAME=T1.MIDDLE_NAME, PREFIX=T1.PREFIX, JOINT_PREFIX=T1.JOINT_PREFIX, NICKNAME=''Other'', NICKNAME.custom=''''L''''eroy''''"
                # WHAT SHOULD BE IN THE DATABASE?  json?
                # print("Name: {}={}".format(key,v))
            # print("appendme: {}\nor in json: {}".format(appendme, json.dumps(appendme)))
        for a in insertdict:
            self._insertintoMergeSelections(a, ', '.join(insertdict[a]))

    def _tablenameandkeysfromdip(self, dipid):
        """return tablename, xkeys for current dupset and dipid"""
        if self.debug > 3: print(f"def _tablenameandkeysfromdip dipid={dipid}")
        sql = f"select tablename, xkeys from {self.tbldip} where id = {dipid}"
        r = self.db.execute_s(sql)
        if r:
            return [r[0]['tablename'], r[0]['xkeys']]
        else:
            return ['','']

    def _insertintoMergeSelections(self, dipid, sql):
        '''using the dipid, find the correct tablename and xkeys and add them to sql and dupset to merge table'''
        if self.debug > 3: print(f"def _insertintoMergeSelections dipid={dipid}, sql={sql}")
        [tablename, xkeys] = self._tablenameandkeysfromdip(dipid)
        params2  = [tablename, xkeys, sql, self.dupset]
        sql2 = """insert into {}
                (tablename, xkeys, sql, dupset) values (?,?,?,?)""".format(self.tblmerges)
        self.db.execute_i_u_d(sql2, params2)

    def _dupsetids(self):
        if self.debug > 3: print("def _dupsetids")
        # sql = """
        # select 
        # D.goodid as id_num
        # , goodid
        # , NameMasterAppID as appid
        # , PartyAppID
        # --, PartyGOID as goid
        # from {did} D
        # join {db}..NameMasterPersonOrgPartyView PV
        # on PV.ID_NUM = D.goodid
        # where dupset = {dupid} and  isnull(goodid,0)>0
        # union
        # select 
        # D.id_num
        # , goodid
        # , coalesce(PV.NameMasterAppID, NM.appid, 0) as appid
        # , isnull(PartyAppID,0)
        # --, isnull(PartyGOID,0) as goid        
        # from {did} D
        # left join {db}..NameMasterPersonOrgPartyView PV
        # on PV.ID_NUM = D.id_num
        # left join tmseply..NameMaster NM
        # on NM.id_num = D.id_num
        # where dupset = {dupid}    
        # """.format(did=self.tbldid, db=self.jdbname, dupid=self.dupset)
        # NameMasterPersonOrgPartyView was unreliable on dupset 116, 4315907,4364183 with no Party record
        # but this SQL was overkill, thinking we'd need GOID someday
        
        sql = """
        select 
        D.goodid as id_num
        , D.goodid
        , N.appid
        from {did} D
        join {nm} N
        on N.ID_NUM = D.goodid
        where dupset = {dupid} and isnull(goodid,0)>0
        union
        select 
        D.id_num
        , D.goodid
        , isnull(N.appid, 0) as appid
        from {did} D
        left join {nm} N
        on N.ID_NUM = D.id_num
        where dupset = {dupid}    
        """.format(did=self.tbldid, db=self.jdbname, dupid=self.dupset, nm=self.tblNameMaster)
        return self.db.execute_s(sql)

    def countdupsinprogressforadupset(self):
        '''return the count of dups in progress for this dupset'''
        if self.debug > 3: print("def _dupsetids")
        sql = """select count(*) as cnt
            from {}
            where dupset = {}
        """.format(self.tbldip, self.dupset)
        r = self.db.execute_s(sql)
        return r[0]['cnt']
    
    def countmissingmergespicked(self):
        if self.debug > 3: print("def countmissingmergespicked")
        sql = """select count(*) as cnt  
            from {} DIP
            left join {} MS
            on MS.tablename = DIP.tablename
            and MS.xkeys = DIP.xkeys
            and MS.dupset = DIP.dupset
            where DIP.category = 'merge'
            and MS.dupset is null
            and DIP.dupset = {}""".format(self.tbldip, self.tblmerges, self.dupset)
        r = self.db.execute_s(sql)
        return r[0]['cnt']

    def update_status(self):
        if self.debug > 3: print("def update_status")
        # self.status = "" # needs keys, pickmerges, magictime
        dipnum = self.countdupsinprogressforadupset()
        if dipnum > 0:
            # check on merge pics
            mergesstillneededcount = self.countmissingmergespicked()
            if mergesstillneededcount:
                self.status = "pickmerges"
            else:
                self.status = 'magictime'
        else:
            self.status = "not prepped"
        # print ("STATS: {}".format(self.status))
    
    def _listalltables(self):
        '''return list of all tables that might need fixing'''
        if self.debug > 3: print("def _listalltables")
        # ID	tablename	xkeys	tablekeyname	mykeyname	tableuniqkey	defaultaction
        # 7747	ETHNIC_RACE_REPORT	SEQ_NUM	ID_NUM	id_num	APPID	shuffle
        sql = "SELECT * from {} order by id".format(self.tblxkeys )
        return self.db.execute_s(sql)

    def _resetDupsInProgress(self, savemerge=False):
        ''' delete and recreate all dip records for this dupset, including merge info unless savemerge=True'''
        if self.debug > 3: print(f"def _resetDupsInProgress savemerge={savemerge}")
        if not savemerge:
            sql = "delete from {} where dupset = {}".format(self.tblmerges, self.dupset)
            self.db.execute_i_u_d(sql)
        sql = "delete from {} where dupset = {}".format(self.tbldip, self.dupset)
        self.db.execute_i_u_d(sql)
        self.status = "not prepped"
        # self.update_formdata_and_status()

    def _idsintable(self, tabledict, ek={}):
        ''' return list the id_num, cnt, category, and other 
        fields that are required for dip entries'''
        if self.debug > 3: print(f"def _idsintable tabledict={tabledict}, ek={ek}")
        eks = ""
        ekw = ""
        category = ""
        goodmatches = 0
        badmatches = 0
        table = tabledict['tablename']
        tkey =  tabledict['tablekeyname']
        mykey = tabledict['mykeyname']
        defaultaction = tabledict['defaultaction']
        ids = ", ".join(map(str, self.ids[mykey])) # for destruction into ans later
        if ek:
            # {k1:v1, k2:v2}
            # eks = k1, k2
            # ekw = "and k1='v1' and k2='v2'"

            # if defaultaction == 'shuffle':
            #     tmp1 =  list(ek.keys())
            #     for a in tmp1:
            #         if 'SEQ' in a.upper():
            #             # del ek[a]
            #             removed_value = ek.pop(a, 'SEQ_WHATEV')
            #             category = "shuffle {}".format(removed_value)
            # elif defaultaction == 'delete':
            #     category = "delete"

            for a in ek.keys():
                eks += ", {}".format(a)
                if ek[a]:
                    ekw += " and {}='{}'".format(a, ek[a])
                else:
                    ekw += " and isnull({},'')=''".format(a)
        
        sql = """select {}{}, count(*) as cnt 
            from {}..{} 
            where {} in ({}) {} group by {} {}""".format(tkey, eks,
            self.jdbname, table, tkey, ids, ekw, tkey, eks)
        # return sql
        ans = self.db.execute_s(sql)
        
        ''' now use ans to extract if the category should be
            shuffle SEQ*** if DIP.defaultaction = 'shuffle' (already set in cateogry)
              UNLESS 0,*, then just update instead of shuffle
            merge, exactly 1 or 0 for each good/bad ID, and at least 2 ids w/ 1, present web merge fields to user
            skip, >=1 goodids, 0 badids, do nothing
            update, everything that isn't a skip, merge, or shuffle
            '''

        if ans: # should always be true
            # even if default action set, skip it if only goodid(s)
            if len(ans) == 1 and ans[0][tkey] == self.ids[mykey][0]:
                defaultaction = 'skip'
            # if there exactly 1 each of more than one ID
            elif not defaultaction and max(d['cnt'] for d in ans) == 1 and len(ans) > 1:
                defaultaction = "merge"
            # if no good ids, default action was shuffle, then just update instead
            elif defaultaction and self.ids[mykey][0] not in [a[tkey] for a in ans]:
                defaultaction = 'update'
            elif not defaultaction:
                defaultaction = 'update'
            ans[0]['category'] = defaultaction # set here, or above
            
            for a in ans:
                ids = ids.replace(str(a[tkey]),str(a['cnt'])) # put the count in place of each ID
            for b in self.ids[mykey]:
                ids = ids.replace(str(b),"0") # remaining IDs must be 0's, just check them all 
            # return a string of [goodid-cnt, first-bad-id-cnt, ...]
            ans[0]['fullcount'] = ids
        
        return ans

    def _findSEQandremovefromstringname(self, d):
        '''string d is a comma separated list of keys
        locate the key containing SEQ, remove it, and return a string of
        the list without that key, and the keyname in a tuple'''
        if self.debug > 3: print(f"def _findSEQandremovefromstringname d={d}")
        y = []
        r = ""
        for a in d.split(','):
            if 'SEQ' not in a.upper():
                y.append(a.strip())
            else:
                r = a
        return (','.join(y) , r)

    def _dupextrakeysuniqvaluekeys(self, tabledict):
        ''' return dict of disinct xkeys sets w/ values located in this table for current ids
        '''
        if self.debug > 3: print(f"def _dupextrakeysuniqvaluekeys tabledict={tabledict}")
        if tabledict['xkeys']:
            xkeys = tabledict['xkeys']
            table = tabledict['tablename']
            tkey =  tabledict['tablekeyname']
            mykey = tabledict['mykeyname']
            ids = ",".join(map(str, self.ids[mykey]))
            sql = """select distinct {} from {}..{} where {} in ({})
                """.format(xkeys, self.jdbname, table, tkey, ids)
            return self.db.execute_s(sql)
        else:
            return []
 
    def _colsfromtable(self, table):
        """return the column_names for this table"""
        if self.debug > 3: print(f"def _colsfromtable table={table}")
        sql = """
        SELECT c.name AS column_name
        FROM {}.sys.tables AS t
        INNER JOIN {}.sys.columns c ON t.OBJECT_ID = c.OBJECT_ID
        WHERE t.name = '{}'""".format(self.jdbname, self.jdbname, table)
        r = self.db.execute_s(sql)
        # print('sql={} and r={}'.format(sql, r))
        return [b['column_name'] for b in r]

    def _ignorefields(self, table):
        """return ignored fields in the table, or a constant list for now"""
        ans = ['APPROWVERSION','CHANGEUSER','CHANGEJOB','CHANGETIME',
        'USER_NAME','JOB_TIME','LAST_UPDATE','JOB_NAME','ARCHIVE_JOB_TIM',
        'ARCHIVE_USER_NAME','ARCHIVE_DATE']
        if table.lower() == "namemaster":
            ans.append("PREFERRED_NAME")
            ans.append("BIRTH_NAME")
        return ans

    def _insertintodiptable(self, tabledict, ek={}):
        ''' insert into BAY_DupsInProgress'''
        if self.debug > 3: print(f"def _insertintodiptable tabledict={tabledict}, ek={ek}")
        # newek = []
        table = tabledict['tablename']
        tkey =  tabledict['tablekeyname']
        mykey = tabledict['mykeyname']
        cnt = tabledict['fullcount']
        category = tabledict['category']
        # approwversion = ""
        sek = json.dumps(ek)
        sql = """insert into {} (dupset, tablename, tablekeyname,
        mykeyname, xkeys, db, cnt, category) VALUES (?,?,?,?,?,?,?,?)
        """.format(self.tbldip)
        params = [self.dupset, table, tkey, mykey, sek, self.jdbname, cnt, category]
        self.db.execute_i_u_d(sql, params=params)

    # def _getDupsInProgressList(self):
    #     """ return list of only this dupset from prep table with good set of data"""
    #     # top 5 for quick way to only look at a few things
    #     sql = """select  DIP.*, DEK.tableuniqkey from {} DIP
    #         join {} DEK on DIP.tablename = DEK.tablename
    #         where dupset = {} order by DIP.ID
    #         """.format(self.tbldip , self.tblxkeys , self.dupset)
    #     return self.db.execute_s(sql)

    def _getDupsInProgressListofMonlyIncompletes(self):
        """ return list of only this dupset from prep table with good set of data"""
        if self.debug > 3: print("def _getDupsInProgressListofMonlyIncompletes")
        # distinct to prevent ORG_TRACKING and any other tables listed twice from causing dups
        sql = """select distinct DIP.*, DEK.tableuniqkey from {} DIP
            join {} DEK on DIP.tablename = DEK.tablename left join {} M 
            on M.tablename = DIP.tablename and M.xkeys = DIP.xkeys and M.dupset = DIP.dupset
            where DIP.dupset = {} and DIP.category = 'merge' and M.dupset is null order by DIP.ID
            """.format(self.tbldip, self.tblxkeys, self.tblmerges, self.dupset)
        return self.db.execute_s(sql)

    def _getDupsInProgressListwithMergeinfoNoSkips(self):
        """ return list of only this dupset from prep table including merge SET info"""
        if self.debug > 3: print("def _getDupsInProgressListwithMergeinfoNoSkips")
        # top 5 for quick way to only look at a few things
        sql = """select distinct DIP.*, DEK.tableuniqkey, MS.sql from {} DIP
            join {} DEK on DIP.tablename = DEK.tablename
            left join {} MS on MS.tablename = DIP.tablename
            and MS.xkeys = DIP.xkeys
            and MS.dupset = DIP.dupset
            where DIP.dupset = {} and category <> 'skip' order by DIP.ID
            """.format(self.tbldip, self.tblxkeys , self.tblmerges, self.dupset)
        return self.db.execute_s(sql)

    def _axkeys(self, xkeys={}): 
        ''' xkeys start like {'YR_CDE': 2020, 'TRM_CDE': 30, 'CRS_CDE': 'CIS  129  26'}
        or maybe {"YR_CDE": 2020, "TRM_CDE": "30", "CRS_CDE": "CIS  129  26"}
        and returns string like "YR_CDE='2020' and TRM_CDE='30' and CRS_CDE='IS  129  26'"
        '''
        if self.debug > 3: print(f"def _axkeys xkeys={xkeys}")
        axkeys = ""
        
        if isinstance(xkeys, str) and xkeys:
            xkeys = json.loads(xkeys)

        if xkeys:
            for k, v in xkeys.items():
                axkeys += " and {} = '{}'".format(k, v)
        return axkeys

    def _detailsfromTableAndxKeys(self, keyname, ids, table, xkeys={}):
        axkeys = self._axkeys(xkeys)
        sql = """select * from {}..{} where {} in ({}) {}
            """.format(self.jdbname, table, keyname, ids, axkeys)
        r = self.db.execute_s(sql)
        return r

    def _cntparts(self, cnt):
        ''' returns the first part, sum of other parts, and boolean of if the 2nd parts are only 0/1's'''
        if self.debug > 3: print(f"def _cntparts cnt={cnt}")
        p2just0and1s = True
        parts = cnt.split(', ')
        intparts = [int(p) for p in parts]
        p1 = intparts[0]
        p2 = sum(intparts[1:])
        for a in intparts[1:]:
            if a > 1:
                p2just0and1s = False
        return p1, p2, p2just0and1s

    def _actionplan(self, cnt, t):
        ''' if tdef, table default, is not specified, categories of cnt and what to do with them:
        0,1...	 BCD
        1...,0	-
        1,[01],[01]... ABCD (no 2's, at least one 1 )
        1,2...   BCD (maybe a, but too much work for nada)
        2,1...	 BCD
        
        A = show boxes and update T0
        B = update all bad id records with good id
        C = insert bad record(s) into new ones with good id
        D = delete all bad ID records
        '''
        if self.debug > 3: print(f"def _actionplan cnt={cnt}, t={t}")
        p1,p2,p2just0and1s = self._cntparts(cnt)
        if p1 == 0 and p2 >= 1:
            return "BCD"
        elif p1 >= 1 and p2 == 0:
            return ""
        elif p1 == 1 and p2 >= 1 and p2just0and1s:
            return "ABCD"
        elif p1 == 1 and p2 >= 2:
            return "BCD"
        elif p1 >= 2 and p2 >= 1:
            return "BCD"
        else:
            return "" # in off chance of 0,0 or whatever

            
    def _catagorizecnt(self, cnt):
        ''' categories of cnt and what to do with them:
        0,1...	 BCD
        1...,0	-
        1,[01],[01]... ABCD (no 2's, at least one 1 )
        1,2...   BCD (maybe a, but too much work for nada)
        2,1...	 BCD
        
        A = show boxes and update T0
        B = update all bad id records with good id
        C = insert bad record(s) into new ones with good id
        D = delete all bad ID records
        '''
        if self.debug > 3: print(f"def _catagorizecnt cnt={cnt}")
        p1,p2,p2just0and1s = self._cntparts(cnt)
        if p1 == 0 and p2 >= 1:
            return "BCD"
        elif p1 >= 1 and p2 == 0:
            return ""
        elif p1 == 1 and p2 >= 1 and p2just0and1s:
            return "ABCD"
        elif p1 == 1 and p2 >= 2:
            return "BCD"
        elif p1 >= 2 and p2 >= 1:
            return "BCD"
        else:
            return "" # in off chance of 0,0

    def _wrapsqlwithcopybutton(self, sql, color="#ffb3ff"):
        pre = '<div class="buttondiv" style="width: 600px; overflow-x: hidden; border-style: solid; border-radius:10px; padding:5px; background-color:{};">\n<pre class="code">'.format(color)
        post = '''</pre>
            <button type="button" onclick="copybutton(this)" onmouseout="renamebutton(this)">-- Copy text</button>
            </div>
            <br>'''
        return ["{}{}{}".format(pre,'\n'.join(sql),post)]
    
    def _buildT(self, tkey, trow, prefix):
        '''return string "T0.key1='blah' and T0.key2='blah2'" for each csv of tkeys
        this will uniqly id this record by use of table keys, not just id/xkeys'''
        if self.debug > 3: print(f"def _buildT tkey={tkey} trow={trow} prefix={prefix}")
        ans = []            

        for k in tkey.split(","):
            if k not in trow:
                for a in trow.keys():
                    if a.lower() == k.lower():
                        k = a
            ans.append("T{}.{}='{}'".format(prefix, k, trow[k]))
        return ' and '.join(ans)

    def _processC(self, table, cols, tkey, mykey, tablekey, allbutgoodid, axkeys=""):
        if self.debug > 3: print("def _processC lots passed")
        tcols = cols[:]
        ttkey = []
        ttval = []
        ignorelist = ['APPROWVERSION', 'AppRowVersion',tkey, tablekey]
        if table == 'NAME_HISTORY':
            ignorelist.append('UDEF_5A_1')
        for a in ignorelist:
            if a in tcols:
                # if a is a key or approver, drop it
                tcols.remove(a)
        # push in the good id in the right spot
        # tcols.remove(tkey)
        ttkey.append(tkey)
        ttval.append(self.ids[mykey][0])
        if table == 'NAME_HISTORY':
            ttkey.append('UDEF_5A_1')
            ttval.append("'DUPFX'")
        fieldnames = '[' + '], ['.join(tcols) + ']'
        ttkeys = ', '.join(ttkey)
        ttvals = ', '.join([str(a) for a in ttval])
        t = """insert into {}..{} ({}, {}) select {}, {} from {}..{} where {} in ({}) {}""".format(self.jdbname, table, ttkeys, fieldnames,
            ttvals, fieldnames, self.jdbname, table, tkey, allbutgoodid, axkeys )
        return [t]
        
    def _loopoverdupsinprogress(self, tablelist):
        ''' prep for jinja2,
        self.formheaderinfo has dupset, goodid, ids --  one per entire dupset
        self.formbodyinfo has form building info for single selction of each merge set
        self.sqlinfo including guts for javascript
        '''
        if self.debug > 3: print("def _loopoverdupsinprogress tablelist passed")
        self.formheaderinfo = {'dupset':self.dupset, 'ids':self.ids}
        gutsdict = {}

        for t in tablelist:
            gutslist = [] # ['field3=T1.field3','f2=T2.f2']
            approwversions = []
            cids = []
            T = []
            foundationsql = []
            tapprowverck = []
            foundationsql = []
            tjoins = []
            tdels = []
            torsB = []
            torsC = []
            table = t['tablename']
            xkeys = json.loads(t['xkeys'])
            tkey =  t['tablekeyname']
            mykey = t['mykeyname']
            tablekey = t['tableuniqkey']
            # maybe a csv list "ID_NUM" or "ID_NUM,DDP_GRP,DDP_SEQ,CALENDAR_YR"

            dipid = t['ID']
            ids = ", ".join(map(str, self.ids[mykey]))
            goodid = self.ids[mykey][0]
            allbutgoodid = ','.join(str(a) for a in self.ids[mykey][1:])

            cols = self._colsfromtable(table) # not redunant (from self.columns)
            cnt= t['cnt'] # double check this and refresh all DIP records if mismatched
            runstuff = self._catagorizecnt(cnt)
            if runstuff:
                A = 'A' in runstuff # show boxes and update T0
                B = 'B' in runstuff # update all bad id records with good id
                C = 'C' in runstuff # insert bad record(s) into new ones with good id
                D = 'D' in runstuff # delete all bad ID records

                # xkeysdict = {}
                # if table == 'NAME_HISTORY':
                #     gutslist.append("UDEF_5A_1='DUPFX'") # WRONG, this table is likely a bulk fix update...
                
                # popuate precheck self.sqlinfo if AAPROWVERSION is present
                # and prep the T01...s for use with A
                p1,p2,p2just0and1s = self._cntparts(cnt)
                details = []
                if p1 > 0:
                    details.extend(self._detailsfromTableAndxKeys(tkey, goodid, table, xkeys))
                if p2 > 0:
                    details.extend(self._detailsfromTableAndxKeys(tkey, allbutgoodid, table, xkeys))
                cids.append(details)

                foundationsql.append("-- INFO: This is the SQL to base decisions on, count={}".format(cnt))
                axkeys = self._axkeys(xkeys)
                if axkeys:
                    axkey = "\n{}".format(axkeys)
                foundationsql.append("-- select * from {}..{} where {} in ({})\n-- {}".format(self.jdbname, table, tkey, ids, axkeys))

                index = 0
                tdels.append("-- D: usually used after a good update")
                tdels.append("-- select * -- delete \n-- from {}..{} where {} in ({})\n-- {}".format(self.jdbname,
                    table, tkey, allbutgoodid, axkeys))
                for tt in details:
                    if 'APPROWVERSION' in tt: # FIX for mixed Case AppRowVersion
                        approwversions.append(tt['APPROWVERSION'])
                    bT =self._buildT(tablekey, tt, index)
                    T.append(bT)
                    index += 1

                axkeys = self._axkeys(xkeys)
                if approwversions:
                    arvcs = ', '.join([str(int.from_bytes(element, 'big')) for element in approwversions]) 
                    # gids = ', '.join([str(tt) for tt in self.ids[mykey]])            
                    tapprowverck.append("""-- (optionally) verify records are still the ones when this report was first run
select '{}' as [table], '{}' as xkeys
, '{}' as 'TheCount', {}, '{}' as allIDsGoodFirst
, case when cast(approwversion as bigint) in ({}) then 'GoodAppRowVersion' else 'BADAPPROWVERSION' end as AppRowCk
from {}..{} where {} in ({}) {}""".format(table,
                        json.dumps(xkeys), len(T), tkey, ids, arvcs,
                        self.jdbname, table, tkey, ids, axkeys))
                else:
                    tapprowverck.append("-- {} has no approwversion for verification".format(table))

                # tapprowverck.append("id counts = {}".format(cnt))
                t = ""

                for index, item in enumerate(T):
                    if index == 0:
                        xset = ""
                        if table == 'NAME_HISTORY': # REDO this the new way?? TODO
                            xset = ",UDEF_5A_1='DUPFX'"
                        t = """-- B: update all bad ids to good id, it works sometimes
-- update {}..{} set {}={}{} where {} in ({})\n-- {} """.format(self.jdbname, table,
                            tkey, goodid, xset, tkey, allbutgoodid, axkeys)
                        torsB.append(t)

                        tjoins.append('-- A: Update based on table selections below\n<span class="guts"></span> from {}..{} T0 '.format(self.jdbname, table))
                            # torsB.append("--item {} =?= {} with key {}".format(item, self.ids, tkey.lower()))
                    else:
                        tjoins.append("--join {}..{} T{} on {}".format(self.jdbname, table, index, item))
                        # need to triple check this works with one bad id
                tjoins.append("--where {} ".format(T[0])) # T0

                ans = []

                if A: # must go first for javascript button and guts to work correctly
                    ans.extend(self._wrapsqlwithcopybutton(tjoins)) # guts are in here somewhere
                
                ans.extend(self._wrapsqlwithcopybutton(foundationsql))
                # ans.extend(self._wrapsqlwithcopybutton(tapprowverck))
                if B:
                    ans.extend(self._wrapsqlwithcopybutton(torsB))
                if C:
                    ans.extend(self._wrapsqlwithcopybutton(self._processC(table, cols, tkey, mykey, tablekey, allbutgoodid, axkeys)))
                if D:
                    ans.extend(self._wrapsqlwithcopybutton(tdels))
                self.sqlinfo[dipid] = "{}".format('\n'.join(ans))


                # -- this is for the javascript filling of each "guts" span with initial field settings,
                # and to set full sql blocks with the dipid key
                # self.sqlinfo = {'guts':"{234:['field3=T1.field3','duh=T2.duh']}",
                #         234:"from NAME_TYPE_TABLE T0 join NAME_TYPE_TABLE T1 on T1.ID_NUM = 4357237 and blah=blah,s=s join NAME_TYPE_TABLE T2 on T2.ID_NUM = 4366909 and blah=blah,s=s where T0.ID_NUM = 436313 and blah=blah,s=s1<br />delete from NM where ID_NUM = 4357237 and blah=blah,s=s1<br />delete from NM where ID_NUM = 4366909 and blah=blah,s=s1"}
                #     headerinfo = {'dupset':29, 'goodid':4363131 , 'ids':[4363131, 4357237, 4366909]}
                
                # -- this sets up the select textbox forms in A, and a single entry works for showing some info
                # self.formbodyinfo =
                # [{'table':'NAME_TYPE_TABLE','field':'id_num','class':'key',
                #     'xkeys':'blah=blah,s=s','dipid':234,'cnt':'1,2,1',
                #     'options':[{'selected':'selected','showval':'4363131','disabled':'disabled'},
                #     {'showval':'4357237','disabled':'disabled'},
                #     {'showval':'4366909','disabled':'disabled'}],
                # , 'custom':'blah' , 'customdisabled':'disabled' }
                #  -- for each field append one like this

                if not A:
                    myformbodyinfo = {}
                    myformbodyinfo['table'] = table
                    myformbodyinfo['xkeys'] = xkeys
                    myformbodyinfo['dipid'] = dipid
                    myformbodyinfo['cnt'] = cnt
                    self.formbodyinfo.append(myformbodyinfo) # short answer, mostly just for dipid
                else: # show full set of select boxes
                    ignorelist = self._ignorefields(table)
                    for col in cols:
                        cl = ""
                        options= []
                        myformbodyinfo = {}

                        myformbodyinfo['table'] = table
                        myformbodyinfo['field'] = col
                        myformbodyinfo['xkeys'] = xkeys
                        myformbodyinfo['dipid'] = dipid
                        myformbodyinfo['cnt'] = cnt

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
                        if 0 in dupck:
                            dupck.remove(0)

                        if cl == 'ignore' or not dupck or (col.upper() == tkey.upper() and cids[0]):
                            options[0]['selected'] = 'selected'
                            cl  = 'auto' # fix to key later if needed

                        elif col.upper() == tkey.upper() and not cids[0]:
                            # default to goodid using the 
                            myformbodyinfo['custom'] = self.goodid
                            gutslist.append("{}={}".format(tkey, self.goodid)) # ticless int
                        
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
                            
                        if (xkeys and col.upper() in (name.upper() for name in xkeys.keys())) or col.upper() == tkey.upper() or col.upper() in (n.upper() for n in tablekey.split(', ')):
                            # disable all options, but not the select or else the required style gets wonky
                            # col in xkeys is a little sloppy, but should be fine, searching field name in SQL
                            for i in range(len(options)):
                                options[i]['disabled'] = 'disabled'
                                options[0]['selected'] = 'selected'
                            cl = 'key'
                            myformbodyinfo['customdisabled'] = 'disabled'

                        myformbodyinfo['class'] = cl
                        myformbodyinfo['options'] = options
                        self.formbodyinfo.append(myformbodyinfo)
                    # include a guts list for every dipid, even if empty, used by javascript
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

    def update_formdata_and_status(self):
        ''' insert lines into dups in progress table with correct xkeys for these ids'''
        if self.debug > 3: print("def update_formdata_and_status")
        # print("def update_formdata_and_status")
        jcols = []
        tablelist = []

        if not self.goodid:
            self.status = "needs human interaction"

        elif self.status == "not prepped":
            # nothing for this dupset, let's fill the BAY_DupsInProgres with every applicable table
            tablelist = self._listalltables()
            tableandxkeysshufduptest = ""
            # tablelist = [{'ID': 2624, 'tablekeyname':'ID_NUM', 'mykeyname':'id_num', 'tablename': 'TW_GRP_MEMBERSHIP', 'xkeys': 'GROUP_ID', 'ID_NUM': 'ID_NUM', 'APPID': None}]
            for t in tablelist:
                table =  t['tablename']
                # tablekeyname	mykeyname
                # ID_NUM	id_num
                xkeys = t['xkeys']
                tkey =  t['tablekeyname']
                mykey = t['mykeyname']
                ids = ", ".join(map(str, self.ids[mykey])) # what this table calls them
                category = ""
                hasmerges = 0

                # ck = self.db.columns
                # cols = self._colsfromtable(table)
                # DON'T DO this if cnt = 0,[>1]
                if t['defaultaction'] == 'shuffle':
                    ans =  self._findSEQandremovefromstringname(xkeys)
                    t['xkeys'] = ans[0]
                    if ans[1]:
                        category = "shuffle {}".format(ans[1])
                    else:
                        category = "BADSHUF" # should never happen, shuffle update limited with xkeys like %seq%
                    t['defaultaction'] = category
                    # stripping off that last xkey causes dups, abort this loop before inserting the dup record
                    if tableandxkeysshufduptest ==  f"{table} {t['xkeys']}":
                        # skip this table entry, it's really  a dup (happens with *seq* removed)
                        break
                    tableandxkeysshufduptest = f"{table} {t['xkeys']}"
                
                extrakeys = self._dupextrakeysuniqvaluekeys(t)
                # extrakeys = [{'YR':2017, 'TRM':10}, {'YR':2017, 'TRM':20}, {'YR':2017, 'TRM':30}]
                if extrakeys:
                    for ek in extrakeys:
                        s = self._idsintable(t, ek)
                        if s:
                            t['fullcount'] = s[0]['fullcount']
                            t['category'] = s[0]['category']
                            self._insertintodiptable(t, ek) # store ek as dictionary
                else:
                    s = self._idsintable(t)
                    # helpme = ""
                    # if s and max([l['cnt'] for l in s]) > 1:
                    #     helpme = {'HELPME':s}
                    if s:
                        t['fullcount'] = s[0]['fullcount']
                        t['category'] = s[0]['category']
                        self._insertintodiptable(t)
            self.status = "pickmerges"

        # check that status again, please 
        if self.status == "pickmerges":
            tablelist = self._getDupsInProgressListofMonlyIncompletes()
            # tablelist = [{'ID':4293, 'db':'TmsEPly', 'dupset':'TmsEPly',
            # 'tablename':'ARCHIVE_NAME_HISTORY','tablekeyname':'ID_NUM',
            # 'mykeyname'	:'id_num','xkeys':'', 'APPROWVERSION':None,
            # 'tableuniqkey':'APPID' }]
            if tablelist:
                self._loopoverdupsinprogress(tablelist)
            else:
                # either no merges, merges stuck from updated without reset, or nothing to do
                self.status = "magictime"

        # if self.status == "magictime":
        #     # everything is set to try to process this beast
        #     self.attempttheupdate()
        
        # else:
        #     # very odd
        #     print("error 799, eject, something is very wrong, status={}".format(self.status))

        # ok, self.formbodyinfo should be filled now, return anything?

    def ckpatternsinerror(self, err):
        '''given err as {'error':Execption}, return pattern match type and extra info as list'''
        if self.debug > 3: print("def ckpatternsinerror")
        patternref = re.compile('REFERENCE constraint.*table[^"]*"([^"]*)"')
        patterndupinsert = re.compile("Cannot insert duplicate key row in object 'dbo.([^']*)'.*index")
        patternconflictfk = re.compile('UPDATE statement conflicted with the FOREIGN KEY constraint .*table "([^"]*)') 
        # patternpkviolation = re.compile("Violation of PRIMARY KEY constraint '([^']*)'.*object '([^']*)'.*value is ([^)]*)")
        patternpkviolation = re.compile("Violation of PRIMARY KEY constraint '.*object '([^']*)'.*value is")
        patternckconstraint = re.compile('conflicted with the CHECK constraint.*table "([^"]*)"')

        e2 = err['error'].args
        if len(e2) < 2:
            return e2[0], 'localhost' # haha
        else:
            e = e2[1]
            rans = []
            ans = patternref.findall(e)
            ans2 = patterndupinsert.findall(e)
            ans3 = patternconflictfk.findall(e)
            ans4 = patternpkviolation.findall(e)
            ans5 = patternckconstraint.findall(e)
            if ans:
                rans.append("patternref")
                for a in ans:
                    rans.append(a)
            elif ans2:
                rans.append("patterndupinsert")
                for a in ans2:
                    rans.append(a)
            elif ans3:
                rans.append("patternconflictfk")
                for a in ans3:
                    rans.append(a)
            elif ans4:
                rans.append("patternpkviolation")
                rans.append(ans4[0])
                # rans.append(e)
                # rans.append(a)
            elif ans5:
                rans.append("patternckconstraint")
                rans.append(ans5[0])
                # rans.append(e)
            else:
                rans.append("No patterns matched")
                rans.append(e)
            return rans

    def _getshufflevariables(self, t):
        '''given table t, return sql results to support an updated merge process'''
        if self.debug > 3: print(f"def _getshufflevariables t={t}")

        # get the last xkey for this tablename
        lxkey = t['category'].split()[1]
        axkeys = self._axkeys(t['xkeys'])
        goodidsandbadids = ', '.join(str(a) for a in self.ids[t['mykeyname']])
        tkey = t['tablekeyname']

        sql = """select {} as tkey, {sf}  as OldSeqNum, '{sf}' as seqfieldname,
        {idn} as oldid
        , (select max( {sf}) from {db}..{tb}  where {idn} in ({gbi}) {axkeys}) as prevmax
        , (select min( {sf}) from {db}..{tb}  where {idn}   in ({gbi}) {axkeys}) as prevmin
        , ROW_NUMBER() OVER(ORDER BY job_time ASC) as NewSeqNumAdder
        from {db}..{tb} where {idn} in ({gbi})
        {axkeys}
        order by NewSeqNumAdder desc
        """.format(t['tableuniqkey'], axkeys=axkeys, sf=lxkey, gbi=goodidsandbadids,
        db=self.jdbname, tb=t['tablename'], idn=t['tablekeyname'])
        # self.formbodyinfo.append("Shuffle SQL: {}".format(sql))
        r = self.db.execute_s(sql)
        return r

    def _erck(self, r, sql):
        if self.debug > 3: print("def _erck r/sql")
        if 'error' in r and r['error']:
            # self.formbodyinfo.append("-- failed update sql: {}".format(sql))
            ans = self.ckpatternsinerror(r)
            # self.formbodyinfo.append(ans)
            eerror = ans[0]
            etable = ans[1]
            # self.formbodyinfo.append("Ok, I need to do something with error {} and table {}".format(eerror, etable))
            return [eerror, etable]
        else:
            # self.formbodyinfo.append(sql)
            return [False,False] # something to signify success, maybe

    # def _finalupdateattempt(self):
    #     for sql in self.finalsql:
    #         pass
    #     sql = ""
    #     r = self.db.execute_i_u_d(sql)
    #     er,et = self._erck(r, sql)
    #     if er:
    #         self.formbodyinfo.append("really, I'm looking into {} and {}".format(er, et))

    def _fillsqlinfo(self, t):
        '''fill self.finalsql list with info from this single table,
        ex: {'try':'AD,CD', 'A':A, 'C':C, 'D':D, 'dipid':dipid, 
        'tablename':tablename, 'axkeys':axkeys, 'info':INFO }'''
        tkey = t['tablekeyname']
        tablename =t['tablename']
        xkeys = json.loads(t['xkeys'])
        axkeys = self._axkeys(xkeys) # and string of xkeys a=b,c=d
        # tableandkeys = "{} with{}".format(tablename, axkeys)
        mykey = t['mykeyname']
        tableuniqkey = t['tableuniqkey']
        # maybe a csv list "ID_NUM" or "ID_NUM,DDP_GRP,DDP_SEQ,CALENDAR_YR"

        dipid = t['ID']
        ids = ", ".join(map(str, self.ids[mykey]))
        goodid = self.ids[mykey][0]
        allbutgoodid = ','.join(str(a) for a in self.ids[mykey][1:])

        cols = self._colsfromtable(tablename) # not redunant (from self.columns)
        cnt= t['cnt'] # double check this and refresh all DIP records if mismatched
        category = t['category']
        # print("looking at tablename {} of cat {}".format(tablename, category))

        # build up all A, B, C, D SQL and
        # one each per dupinprog: {'try':['AD','CD'], 'A':'update...', 'C':'...', 'D', 'tryagainafter':[]}
        # then send this to wrapped stuff and try it

        # delete
        A=B=C=D=""
        ttt = {}
        C = self._processC(tablename, cols, tkey, mykey, tableuniqkey, allbutgoodid, axkeys)
        D = "delete from {}..{} where {} in ({}) {}".format(self.jdbname,tablename, tkey, allbutgoodid, axkeys)
        INFO = "select * from {}..{} where {} in ({}) {}".format(self.jdbname,tablename, tkey, ids, axkeys)
        if category == 'delete':
            # self.formbodyinfo.append("-- catDELETE {}".format(t))
            ttt = {'try':'D,CD', 'D':D, 'C':C, 'dipid':dipid, 'tablename':tablename, 'axkeys':axkeys, 'info':INFO }
            # print("DELETE found on {}, appending {}".format(tablename, ttt))
            self.finalsql.append(ttt)

        # update
        elif t['category'] == 'update':
            ttt = {}
            xset = ""
            if tablename == 'NAME_HISTORY':
                xset = ",UDEF_5A_1='DUPFX'"
            # self.formbodyinfo.append("-- catUPDATE {}".format(t))
            # tkey 
            B =f"update {self.jdbname}..{tablename} set {tkey} = {goodid} {xset} where {tkey} in ({allbutgoodid}) {axkeys}"
            ttt = {'try':'B,CD', 'B':B, 'C':C, 'D':D, 'dipid':dipid, 'tablename':tablename, 'axkeys':axkeys, 'info':INFO }
            # print("DELETE found on {}, appending {}".format(tablename, ttt))
            self.finalsql.append(ttt)                

        # shuffle
        elif category[:7] == 'shuffle':
            myshuf = []
            ttt = {}
            # self.formbodyinfo.append("-- catSHUFFLE {}".format(t))
            tableuniqkey = t['tableuniqkey']
            # xkeys = t['xkeys'] # *SEQ* already removed
            
            # lastxkey = ""
            gmv = self._getshufflevariables(t) # get extra SQL for merge row updates
            if gmv:
                fldn = gmv[0]['seqfieldname']
                pmax =  gmv[0]['prevmax'] # start here + 1
                pmin =  gmv[0]['prevmin'] # reset low num back here
                # Maybe this logic could be correctly developed, but the following fails because the ID_NUM still needs to be updated on some records
                # if pmax-pmin != gmv[0]['NewSeqNumAdder']-1:
                allwheres = []
                # first go through and update them them up from safe value, 1... + the highest existing one
                for a in gmv:
                    # print(f"processing A: {a}")
                    newseqnum = a['NewSeqNumAdder'] + pmax
                    oldseqnum = a['OldSeqNum']
                    thiswhere = []
                    if "," in tableuniqkey: 
                        for temp in tableuniqkey.split(","): # not redundant
                            if temp != fldn:
                                thiswhere.append("{}='{}'".format(temp, newseqnum))
                    else:
                        thiswhere.append("{}='{}'".format(tableuniqkey, a['tkey']))
                    thiswhereand = ' and '.join(thiswhere)
                    allwheres.append(thiswhereand) # gather values for final update correction

                    idonrec = a['oldid']
                    # fldn = a['seqfieldname'] # little redundant
                    tmp = """update {}..{} set {} = {}, {} = {} where {} -- formerly {} with id {}""".format(self.jdbname,
                        tablename, fldn, newseqnum, tkey, goodid, thiswhereand, oldseqnum, idonrec )
                    myshuf.append(tmp)
            
                # now do am offset correction to reduce the seq_num to start with pmin and go up by ones
                offset = pmax + 1 - pmin
                if offset > 0:
                    myshuf.append("update {}..{} set {fldn} = {fldn} - {offset} where ({}) -- just bumping the seq down by {offset}".format(self.jdbname,
                        tablename, ') or ('.join(allwheres), fldn=fldn, offset=offset ))
                ttt = {'try':'S', 'S':myshuf, 'dipid':dipid, 'tablename':tablename, 'axkeys':axkeys, 'info':INFO }
                # print("Shuffle found on {}, appending {}".format(tablename, ttt))
                self.finalsql.append(ttt)

        # merge
        elif category == 'merge':
            '''use formfield completed info to merge these two records'''
            # self.formbodyinfo.append("-- catMERGE {}".format(t))
            A = ""
            ttt = {}
            if t['sql'] or tablename == 'NAME_HISTORY':
                # prep some variables, T0, T1... especially
                tableuniqkey2 = tableuniqkey.lower().replace(".id_num","").replace("id_num.","") # already handled
                # all messed up if tableuniqkey is CSV
                tar = []
                for tus in tableuniqkey2.split(","):
                    tar.append("'T#.{t}='''+cast({t} as varchar(max))+''''".format(t=tus))
                tarjoin = "+' AND '+".join(tar)
                myid = 0 # get from query
                axkeys = "" # "and ..." from xkeys from t['xkeys'] == xkeys??
                sql = """select {} as idn, {} as dkey from {}..{} where {} in ({}) {} order by 1
                """.format(tkey, tarjoin, self.jdbname, tablename, tkey, ids, axkeys)
                ans = self.db.execute_s(sql)
                # self.formbodyinfo.append(sql)
                T = 0
                tjoins = []
                for a in ans:
                    if a['idn'] == goodid:
                        myid = a['dkey'].replace("T#","T0")
                    else:
                        T += 1
                        ttarjoin = a['dkey'].replace("T#","T{}".format(T))
                        tjoins.append("join {}..{} T{T} on {}".format(self.jdbname, tablename, ttarjoin, T=T))
                #  'sql': 'TRM_HRS_EARNED=T1'
                lsql = t['sql'].split(', ')
                # lsqln = []
                # for a in t['sql'].split(', '):
                #     lsql
                # GRADE_PRT_CONTROL=T1

                if tablename == 'NAME_HISTORY':
                    if lsql == ['']:
                        lsql = ["UDEF_5A_1='DUPFX'"]
                    else:
                        lsql.append("UDEF_5A_1='DUPFX'")
                # update T0 set TOTAL_HRS_HISTORY=T1.TOTAL_HRS_HISTORY,
                #  TOTAL_CLOCK_HRS_HISTORY=T1.TOTAL_CLOCK_HRS_HISTORY, 
                # IPEDS_HS_GRAD_12_MONTHS=T1.IPEDS_HS_GRAD_12_MONTHS
                #  from tmseply..IR_STUDENT_DATA T0 
                # join tmseply..IR_STUDENT_DATA T1 on T1.SNAPSHOT_ID,ID_NUM=4358712 
                # where T0.SNAPSHOT_ID,ID_NUM=4301346

                A = """update T0 set {} from {}..{} T0 {} where {}""".format(', '.join(lsql),
                self.jdbname, tablename, ' '.join(tjoins), myid )
                # self.formbodyinfo.append(sql)
                # r = self.db.execute_i_u_d(sql)
                # self._erck(r, sql)
                # if sql ok, do delete

            # delete bad id, no A, just D
            # D = """delete from {}..{} where {} in ({}) {}
            # """.format(self.jdbname, tablename, tkey, allbutgoodid, axkeys)
            # r = self.db.execute_i_u_d(sql)
            # er,et = self._erck(r, sql)
            # if er:
            #     self.formbodyinfo.append("really, I'm looking into {} and {}".format(er, et))
            if A:
                # if AD  fails, then CD isn't really perfect as it just copies the record, but CAD probably doesn't work because the new APPIDs wouldn't be right
                ttt = {'try':'AD,CAD', 'A':A, 'C':C, 'D':D, 'dipid':dipid, 'tablename':tablename, 'axkeys':axkeys, 'info':INFO }
            else:
                ttt = {'try':'D', 'D':D, 'dipid':dipid, 'tablename':tablename, 'axkeys':axkeys, 'info':INFO }
            # print("DELETE found on {}, appending {}".format(tablename, ttt))
            self.finalsql.append(ttt)

        # skip
        elif category == 'skip':
            pass
            # self.formbodyinfo.append("-- catSKIP -- skipping record as only good IDs are present: {}".format(t))
        
        else:
            msg = "-- catWHATSUP (unknown category, very odd) {}".format(t)
            self.formbodyinfo.append(msg)
            # print(msg)
                    
    def attempttheupdate(self):
        if self.debug > 3: print("def attempttheupdate")

        # outofresets used to flag that everything done, then a reset was done, and nothing more to do
        outofresets = False
        self.formbodyinfo.append("-- {} dupset:{} ids:{}".format(datetime.now(), self.dupset, self.ids))
        timeslooped = 1
        triedresettingdatabaserecently = False

        while (not outofresets):
            # grab the tables, and see where we are at go get started
            tablelist = self._getDupsInProgressListwithMergeinfoNoSkips()
            if not tablelist:
                # self.formbodyinfo.append("-- process complete, nothing more to do!")
                # self._appendformbodyinfointofile()
                outofresets = True
            elif self.status != "magictime":
                self.formbodyinfo.append("-- Sorry, but I need to give up now, I've circled back around and created a merge that has not been addressed.  Please reload this page and pick your merges to continue, reset only if you want to.")
                outofresets = True
            else:
                # reset self.finalsql
                self.finalsql = []
                for t in tablelist:
                    self._fillsqlinfo(t)

                # Loop over all dipids (self.finalsql rows)
                # Attempt all "try" action sets of each dipid, in order
                # Add sql of success/failure to webpage & file via self.formbodyinfo
                # Remove success from list, note that not stalled out
                # Repeat until problems are gone or stalled out.
                # On stall or completion, reset db keeping merge info, check again.
                # Give up if stall out immediatly following fresh reset
                # Do something with the noted violation table if stalled out???? TODO
                givingupcompletelyoralldone = False

                while (not givingupcompletelyoralldone):
                    # print("while not giving up")
                    deletedrows = []
                    for finalsqlkey in range(len(self.finalsql)):
                        # print(f"finalsqlkey={finalsqlkey} with val {self.finalsql[finalsqlkey]['dipid']}")
                        a = self.finalsql[finalsqlkey]
                        # each row: {'try':'AD,D', 'D':D,'A':A, 'dipid':dipid, 'tablename':tablename,
                        #   'axkeys':axkeys, 'info':'-- sele..'}
                        # try logic EGH, EFGH, IEFGH 
                        # EGH = try E, if success add to output, remove E from entire string and try next letter in set (G)
                        #  if set completed without errors, rest of set can be ignored, add id to delete pile
                        # if any part of set fails, continue to next set, EFGH, if E failed, you know this will fail
                        # if all sets fail, update logic with removed letters, if any, and don't throw on delete pile
                        tryi = 0
                        keylistchanges = False
                        keylist = a['try']
                        self.formbodyinfo.append(f"---- keylist: {keylist}")
                        trymax = keylist.count(",") # index of highest/last one when split
                        stilltrying = True
                        failedletters = []
                        # print("full keylist: {} (vs a['try'] of {})".format(keylist, a['try']))
                        while (keylist and stilltrying):
                            # cont = input("while stilltrying  keylist {} and stilltrying {} : Hit enter to continue... ".format(keylist,stilltrying))
                            sqlbroke = False
                            trythisset = keylist.split(',')[tryi]
                            for ttl in list(trythisset): # ttl = try this letter
                                if sqlbroke or not stilltrying or ttl in failedletters:
                                    break
                                if ttl == '':
                                    # the only way to get here is to have something like 'AB,A'
                                    # where the A is OK, making it 'B,'
                                    # DON'T build things this way, it doesn't make sense
                                    pass
                                else:
                                    # for S, this is a list of sql commands, otherwise a string of one sql command per try letter
                                    sqllist = a[ttl] 
                                    if isinstance(sqllist, str):
                                        sqllist = [sqllist]
                                    for sql in sqllist:
                                        if not sqlbroke:
                                            r = self.db.execute_i_u_d(sql)
                                            if 'error' in r and r['error']:
                                                ans = self.ckpatternsinerror(r)
                                                eerror = ans[0]
                                                etable = ans[1]
                                                self.formbodyinfo.append("-- failed sql: {} -- errormsg: {} -- table: {}".format(sql, eerror, etable))
                                                sqlbroke = True
                                                tryi += 1
                                                if tryi > trymax:
                                                    stilltrying = False
                                                failedletters.append(ttl)
                                            else:
                                                # print("worked")
                                                
                                                # extra alert here if it was only D in set, and there were previous sets
                                                if tryi <= trymax:
                                                    tmpsets = keylist.split(',')[tryi] # trythisset can't be used here-if keylist changed
                                                    if len(tmpsets) > 1 and ttl == trythisset == tmpsets[-1] == 'D':
                                                        self.formbodyinfo. append("-- I finally gave up on this one and just deleted it, you might look it over closer")
                                                
                                                # add INFO once, on shuffles, this stops multiple entries
                                                if sqllist[0] == sql:
                                                    self.formbodyinfo.append("-- info -- {}".format(a['info']))

                                                # add to output for GOOD sql, yeah, no -- leader
                                                self.formbodyinfo.append(sql)
                                                keylist = keylist.replace(ttl, "")
                                                keylistchanges = True
                            if not sqlbroke:
                                stilltrying = False
                                deletedrows.insert(0, finalsqlkey) # insert at front, now this can be run in order to remove later
                            elif keylistchanges:
                                self.finalsql[finalsqlkey]['try'] = keylist # reset may still cause dups here, unavoidable
                                    
                    for a in deletedrows:
                        # self.formbodyinfo.append("---- deleting row from finalsql: {}".format(a))
                        del self.finalsql[a]
                        keylistchanges = True

                    timeslooped += 1
                    if timeslooped > 15:
                        # sanity check, it should never take this long
                        # self.formbodyinfo.append("---- timeslooped > 15 (givingupcompletelyoralldone=True)")
                        givingupcompletelyoralldone = True
                        outofresets = True

                    elif not self.finalsql and triedresettingdatabaserecently:
                        # all done, no SQL left to processs, and just tried resetting dupid
                        # self.formbodyinfo.append("---- not self.finalsql and triedresettingdatabaserecently (givingupcompletelyoralldone = True)")
                        givingupcompletelyoralldone = True
                        outofresets = True
                        
                    elif not self.finalsql and not triedresettingdatabaserecently:
                        # let's reset the database, just in case there is more in NAME_HISTORY or somewhere
                        # self.formbodyinfo.append("---- not self.finalsql and not triedresettingdatabaserecently (reset and triedresettingdatabaserecently = True)")
                        givingupcompletelyoralldone = True


                    elif not keylistchanges and not triedresettingdatabaserecently:
                        # no sql succeeded through an entire pass, but havn't tried resetting the database
                        # self.formbodyinfo.append("---- not keylistchanges and not triedresettingdatabaserecently (reset and triedresettingdatabaserecently = True)")
                        givingupcompletelyoralldone = True

                    elif not keylistchanges and triedresettingdatabaserecently:
                        # self.formbodyinfo.append("---- not keylistchanges and triedresettingdatabaserecently (givingupcompletelyoralldone = True)")
                        # no sql succeeded in a pass, and we just tried a databse refresh also
                        givingupcompletelyoralldone = True
                        outofresets = True

                    elif keylistchanges:
                        # sql got somewhere, don't quit yet
                        triedresettingdatabaserecently = False
                        # self.formbodyinfo.append("---- keylistchange (triedresettingdatabaserecently = False)")
                    
                    self.formbodyinfo.append(f"-- ROUND {timeslooped} not self.finalsql = {not self.finalsql}, keylistchanges = {keylistchanges}, triedresettingdatabaserecently = {triedresettingdatabaserecently}, givingupcompletelyoralldone {givingupcompletelyoralldone} outofresets {outofresets}")
                    # print(f"-- ROUND {timeslooped} not self.finalsql = {not self.finalsql}, keylistchanges = {keylistchanges}, triedresettingdatabaserecently = {triedresettingdatabaserecently}, givingupcompletelyoralldone {givingupcompletelyoralldone} outofresets {outofresets}")

            if not outofresets:
                self._resetDupsInProgress(savemerge=True)
                self.update_formdata_and_status()
                triedresettingdatabaserecently = True
                self.formbodyinfo.append("---- resetting database to check again")

        tableprobs =set([a['tablename'] for a in self.finalsql])
        used = set()
        u = [x for x in self.finalsql if x not in used and (used.add(x) or True)]
        self.formbodyinfo.append(f"""-- Done processing, 
            -- timeslooped: {timeslooped}
            -- remaining len(finalsql): {len(self.finalsql)} 
            -- tables with probs: {tableprobs}, finalsql: {u}""")
        self._appendformbodyinfointofile()


    def _appendformbodyinfointofile(self):
        '''append to the goodid.sql.txt file all actions taken and the date'''
        if self.debug > 3: print("def _appendformbodyinfointofile")
        fn = "SQLActions\\{}.sql.txt".format(self.goodid)
        
        with open(fn, 'a') as file1:
            for a in self.formbodyinfo:
                # if the reset table process passes back through, self.formbody will have dictionary entries, maybe we can just ignore them???
                if isinstance(a, str) and a:
                    # print(f"HEY: {a}")
                    file1.write(a)
                    file1.write('\n')

    def _automerge(self):
        '''auto fill all merge with the no updates'''
        if self.debug > 3: print("def _automerge")
        # this does not protect against duplicate insertions (yet) TODO
        sql = f"insert into {self.tblmerges} (sql, dupset, tablename, xkeys) select '', dupset, tablename, xkeys from {self.tbldip} where  category = 'merge' and dupset = {self.dupset}"
        self.db.execute_i_u_d(sql)


###########################
#the below is a manual test.
if __name__ == '__main__':
    fd = {}
    # fd = {'68349.MIDDLE_NAME': 'T1',
    #     '68349.PREFIX': "T1",
    #     '68349.JOINT_PREFIX': "T1",
    #     '68349.NICKNAME': "Other",
    #     '68349.NICKNAME.custom': "''L''''eroy''"}
    #     # "... JOINT_PREFIX=T1.JOINT_PREFIX, NICKNAME=''Other'', NICKNAME.custom=''''L''''eroy''''"
    # fd = {
    #     '71659.SELF_REPORTED_GRADUATION_DTE': 'T1'
    # }
    # fd = {'295200.UDEF_5A_2':'Other','295200.UDEF_5A_2.custom':'6A#@%'}
    # for a in range(2,27):
    a = 9
    print(f"Trying {a} at {datetime.now()}")
    sumpin = Dupset(a, fd)
    print(f"goodid {sumpin.goodid}")
    # print("ok1")
    # sumpin._resetDupsInProgress(savemerge=True)
    sumpin._resetDupsInProgress() # erases category=merge sql and dip
    sumpin.update_formdata_and_status()
    sumpin._automerge()
    sumpin.update_formdata_and_status()
    # print("ok3")
    print(sumpin.status)
    if sumpin.status == "magictime":
        sumpin.attempttheupdate()
        print(sumpin.formbodyinfo) # [-1])
        # print(sumpin.formheaderinfo)
    # print("ok4")
    # # print(f"HEAD: {sumpin.formheaderinfo}")
    # print(f"FBI: {sumpin.formbodyinfo}")

# from flaskr.models.dbsetup import Dbsetup
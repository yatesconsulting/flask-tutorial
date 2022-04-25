#  this might only be needed when running from the command line, testing
from os import replace
import sys
import json
import re
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial/flaskr') # required for from flaskr.models.... import
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial') # required for ?import?
# from flaskr import pyodbc_db
from flaskr import pyodbc_db
from myflaskrsecrets import dbname ## mcn_connect for Mesalands

class Dupset():
    def __init__(self, dupset):
        self.dupset = dupset # number of the duplicate set of 2 or more IDs to merge
        self.jdbname = "tmseply" # always run against play, to generate script for use on live after verification
        self.db = pyodbc_db.MSSQL_DB_Conn() # maybe this is the way to only use one db connection?
        self.dbname = dbname
        # my custom tables, database is probalby wrong for non-Mesalands users
        self.tbldid = '{}..BAY_DupIDs'.format(dbname)
        self.tbldip = '{}..BAY_DupsInProgress'.format(dbname)
        self.tblxkeys = '{}..BAY_DupExtraKeys'.format(dbname)
        self.tblmerges = '{}..BAY_dipidMergeSelections'.format(dbname)
        self.tblPKConstraints = '{}..BAY_PKConstraints'.format(dbname)
        self.ids = {}
        self.goodid = 0
        self.goodappid = 0
        # self.goodgoid = 0
        self.formheaderinfo = {}
        self.formbodyinfo = []
        self.sqlinfo = {}
        self.status = "" # needs keys, pickmerges, magictime
        self.update_status()
        # self.count11selectionscomplete = False

        # sql = """select distinct db from {}..BAY_DupIDs""".format(dbname)
        # cnt = 0
        # try:
        #     # r = self.db.execute_s(sql)
        #     r = self.db.execute_s(sql)
        #     cnt = self.db.record_count 
        # except Exception as e:
        #     print("exception {}".format(e.args[1]))
        # if cnt != 1:
        #     self.formbodyinfo.append("Something is wrong, please rebuild the dup list, exactly 1 databases required.")
        #      # just do it
        # else:
        #     self.jdbname = r[0]['db']
        
        dupids = self._dupsetids() # id_num, goodid for each in a given dupset
        # print('dupids {}'.format(dupids))
        if dupids and not dupids[0]['goodid']:
            self.formbodyinfo.append("ERROR: missing goodid")
            self.formbodyinfo.append("Please identify the goodid and refresh duplist first (dupset {})".format(dupset))
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

            self.update_formdata()

        else:
            self.formbodyinfo.append("dupset {} had nothing to look at".format(dupset))

    def _dupsetids(self):
        sql = """
        select 
        D.goodid as id_num
        , goodid
        , NameMasterAppID as appid
        , PartyAppID
        --, PartyGOID as goid
        from {did} D
        join {db}..NameMasterPersonOrgPartyView PV
        on PV.ID_NUM = D.goodid
        where dupset = {dupid} and  isnull(goodid,0)>0
        union
        select 
        D.id_num
        , goodid
        , coalesce(PV.NameMasterAppID, NM.appid, 0) as appid
        , isnull(PartyAppID,0)
        --, isnull(PartyGOID,0) as goid        
        from {did} D
        left join {db}..NameMasterPersonOrgPartyView PV
        on PV.ID_NUM = D.id_num
        left join tmseply..NameMaster NM
        on NM.id_num = D.id_num
        where dupset = {dupid}    
        """.format(did=self.tbldid, db=self.jdbname, dupid=self.dupset)
        return self.db.execute_s(sql)

    def countdupsinprogressforadupset(self):
        sql = """select count(*) as cnt
            from {}
            where dupset = {}
        """.format(self.tbldip, self.dupset)
        r = self.db.execute_s(sql)
        return r[0]['cnt']
    
    def countmissingmergespicked(self):
        sql = """select count(*) as cnt  
            from {} DIP
            left join {} MS
            on MS.dipid = dip.ID
            where DIP.category = 'm'
            and MS.dipid is null
            and DIP.dupset = {}""".format(self.tbldip, self.tblmerges, self.dupset)
        r = self.db.execute_s(sql)
        return 1 # for now, TODO FIX
        return r[0]['cnt']

    def update_status(self):
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
    
    def _listalltables(self):
        '''return list of all tables that might need fixing'''
        # ID	tablename	xkeys	tablekeyname	mykeyname	tableuniqkey	defaultaction
        # 7747	ETHNIC_RACE_REPORT	SEQ_NUM	ID_NUM	id_num	APPID	shuffle
        sql = "SELECT * from {} order by id".format(self.tblxkeys )
        return self.db.execute_s(sql)

    def _resetDupsInProgress(self):
        ''' delete and recreate all dip records for this dupset'''
        # delete from [BAY_dipidMergeSelections] first, then... TODO
        sql = "delete from {} where dupset = {}".format(self.tbldip, self.dupset)
        self.db.execute_i_u_d(sql)
        self.status = "not prepped"
        self.update_formdata()

    def _idsintable(self, tabledict, ek={}):
        # return list the id_num count, and any other fields that are required for actual dip entries
        eks = ""
        ekw = ""
        table = tabledict['tablename']
        tkey =  tabledict['tablekeyname']
        mykey = tabledict['mykeyname']
        ids = ", ".join(map(str, self.ids[mykey])) # for destruction into ans later
        if ek:
            # {k1:v1, k2:v2}
            # eks = k1, k2
            # ekw = "and k1='v1' and k2='v2'"

            # if defaultaction == 'shuffle':
            # remove last %seq% field from 

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
        if ans:
            for a in ans:
                ids = ids.replace(str(a[tkey]),str(a['cnt']))
            for b in self.ids[mykey]:
                ids = ids.replace(str(b),"0")
            # return a string of [goodid-cnt, first-bad-id-cnt, ...]
            ans[0]['fullcount'] = ids
        
        ''' now use ans to extract if the category should be
            merge, 1, [01] ...
            shuffle if DIP.defaultaction = 'shuffle'
            '''
        return ans

    def _dupextrakeysuniqvaluekeys(self, tabledict):
        ''' return list, id_num count, and any other fields that are required for key2 generation
        tabledict may have xkeys in CSV form already
        '''
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
        newek = []
        table = tabledict['tablename']
        tkey =  tabledict['tablekeyname']
        mykey = tabledict['mykeyname']
        cnt = tabledict['fullcount']
        approwversion = ""
        sek = json.dumps(ek)
        sql = """insert into {}
        (dupset, tablename, tablekeyname, mykeyname, xkeys, db, cnt) VALUES ({},'{}','{}','{}','{}','{}','{}')
        """.format(self.tbldip , self.dupset, table, tkey, mykey, sek, self.jdbname, cnt)
        self.db.execute_i_u_d(sql)

    def _getDupsInProgressList(self):
        """ return list of only this dupset from prep table with good set of data"""
        # top 5 for quick way to only look at a few things
        sql = """select  DIP.*, DEK.tableuniqkey from {} DIP
            join {} DEK on DIP.tablename = DEK.tablename
            where dupset = {} order by DIP.ID
            """.format(self.tbldip , self.tblxkeys , self.dupset)
        return self.db.execute_s(sql)
    
    def _getDupsInProgressListofMonly(self):
        """ return list of only this dupset from prep table with good set of data"""
        # top 5 for quick way to only look at a few things
        sql = """select  DIP.*, DEK.tableuniqkey from {} DIP
            join {} DEK on DIP.tablename = DEK.tablename
            where dupset = {} and DIP.category = 'm' order by DIP.ID
            """.format(self.tbldip, self.tblxkeys, self.dupset)
        return self.db.execute_s(sql)

    def _axkeys(self, xkeys={}):
        ''' xkeys start like {'YR_CDE': 2020, 'TRM_CDE': 30, 'CRS_CDE': 'CIS  129  26'}
        and returns string like "YR_CDE='2020' and TRM_CDE='30' and CRS_CDE='IS  129  26'"
         '''
        axkeys = ""
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
        p2just0and1s = True
        parts = cnt.split(', ')
        intparts = [int(p) for p in parts]
        p1 = intparts[0]
        p2 = sum(intparts[1:])
        for a in intparts[1:]:
            if a > 1:
                p2just0and1s = False
        return p1, p2, p2just0and1s

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
        ans = []            

        for k in tkey.split(","):
            if k not in trow:
                for a in trow.keys():
                    if a.lower() == k.lower():
                        k = a
            ans.append("T{}.{}='{}'".format(prefix, k, trow[k]))
        return ' and '.join(ans)

    def _processC(self, table, cols, tkey, mykey, tablekey, allbutgoodid, axkeys=""):
        tcols = cols[:]
        ttkey = []
        ttval = []
        for a in 'APPROWVERSION, AppRowVersion, {}, {}'.format(tkey, tablekey).split(", "):
            if a in tcols:
                # if a is a key or approver, drop it
                tcols.remove(a)
        # push in the good id in the right spot
        # tcols.remove(tkey)
        ttkey.append(tkey)
        ttval.append(self.ids[mykey][0])
        fieldnames = '[' + '], ['.join(tcols) + ']'
        ttkeys = ', '.join(ttkey)
        ttvals = ', '.join([str(a) for a in ttval])
        t = """-- C: huge ugly way, recreate new ids with old data and new appids
-- insert into {}..{} ({}, {})
-- select {}, {}
-- from {}..{} where {} in ({}) {}""".format(self.jdbname, table, ttkeys, fieldnames,
            ttvals, fieldnames, self.jdbname, table, tkey, allbutgoodid, axkeys )
        return [t]
        
    def _loopoverdupsinprogress(self, tablelist):
        # for jinja2, self.formheaderinfo has dupset, goodid, ids --  one per entire dupset
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
                #     gutslist.append("UDEF_5a_1='DUPFX'") # WRONG, this table is likely a bulk fix update...
                
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
                        if table == 'NAME_HISTORY':
                            xset = ",UDEF_5a_1='DUPFX'"
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

    def update_formdata(self):
        jcols = []
        tablelist = []

        if self.status == "not prepped":
            # nothing for this dupset, let's fill the BAY_DupsInProgres with every applicable table
            tablelist = self._listalltables()
            # tablelist = [{'ID': 2624, 'tablekeyname':'ID_NUM', 'mykeyname':'id_num', 'tablename': 'TW_GRP_MEMBERSHIP', 'xkeys': 'GROUP_ID', 'ID_NUM': 'ID_NUM', 'APPID': None}]
            for t in tablelist:
                table =  t['tablename']
                # tablekeyname	mykeyname
                # ID_NUM	id_num
                xkeys = t['xkeys']
                tkey =  t['tablekeyname']
                mykey = t['mykeyname']
                ids = ", ".join(map(str, self.ids[mykey])) # what this table calls them

                # ck = self.db.columns
                # cols = self._colsfromtable(table)
                if t['defaultaction'] == 'shuffle':
                    pass # do something different?

                extrakeys = self._dupextrakeysuniqvaluekeys(t)
                # extrakeys = [{'YR':2017, 'TRM':10}, {'YR':2017, 'TRM':20}, {'YR':2017, 'TRM':30}]
                if extrakeys:
                    # CHECK for mergeall flag here TODO
                    for ek in extrakeys:
                        s = self._idsintable(t, ek)
                        # print("ek={} s={}".format(ek, s))
                        # if s and max([l['cnt'] for l in s]) > 1:
                        #     helpme = {'HELPME':s}                        
                        # _buildformdetaillines(jdbname, table, s, ids, cols, ek)
                    # if s and helpme:
                    #     self._insertintodiptable(t, helpme)
                    # elif s:
                    #     for ek in extrakeys:
                        if s:
                            t['fullcount'] = s[0]['fullcount']
                            self._insertintodiptable(t, ek) # store ek as dictionary
                else:
                    s = self._idsintable(t)
                    # helpme = ""
                    # if s and max([l['cnt'] for l in s]) > 1:
                    #     helpme = {'HELPME':s}
                    if s:
                        t['fullcount'] = s[0]['fullcount']
                        self._insertintodiptable(t)
                # except Exception as e:
                #     pass # skip tables with errors in xkeys TODO fix
            # self.update_status()
            self.status = "pickmerges"

        # READY, ok, now let's eith
        if self.status == "pickmerges":
            # must all be OK, let's do it for real
            tablelist = self._getDupsInProgressList()
            # tablelist = [{'ID':4293, 'db':'TmsEPly',
            # 'dupset':'TmsEPly',
            # 'tablename':'ARCHIVE_NAME_HISTORY',
            # 'tablekeyname':'ID_NUM',
            # 'mykeyname'	:'id_num',
            # 'xkeys':'',
            # 'APPROWVERSION':None,
            # 'tableuniqkey':'APPID' }]
            self._loopoverdupsinprogress(tablelist)

        elif self.status == "magictime":
            pass # the big dog goes here
        else:
            # very odd
            print("error 511, eject, something is very wrong, status={}".format(self.status))

        # ok, self.formbodyinfo should be filled now, return anything?
    def _rebuildupextrakeys(self):
        # tbl = "MCN_Connect..BAY_DupExtraKeys"
        # con = "MCN_Connect..BAY_PKConstraints"
        sql = """
        delete from {tbl}
        insert into {tbl}
        (tablename,xkeys,tablekeyname,mykeyname,tableuniqkey)
        values 
        --('VENDOR_WEB_SITE','URL','ID_NUM','id_num','APPID')
        --,('VNDR_MASTER',NULL,'ID_NUM','id_num','APPID')
        --,('NameMaster',NULL,'IDorg_tracking _NUM','id_num','APPID')
        --,('NAME_HISTORY',NULL,'ID_NUM','id_num','APPID')
        ('ORG_TRACKING','ORG_ID_NUM','ID_NUM','id_num','APPID') -- UI,ORG_TRACKING , SEQ_NUM
        ,('ORG_TRACKING',NULL,'ORG_ID_NUM','id_num','APPID') -- UI,ORG_TRACKING , SEQ_NUM
        ,('AD_ORG_TRACKING','ORG_ID__AD','ID_NUM','id_num','APPID') -- from UniqueIndexes,AD_ORG_TRACKING, was SEQ_NUM
        ,('AD_ORG_TRACKING',NULL,'ORG_ID__AD','id_num','APPID') -- MAYBE, as id_num of orgs, not person TODO check this
        ,('ADV_MASTER',NULL,'ID_NUM','id_num','APPID') -- changed per UI,ADV_MASTER
        ,('ADVISING_HISTORY','ADVISOR_ID,SEQ_NUM','ID_NUM','id_num','APPID') -- ADVISING_HISTORY

        ,('AlternateNameMasterNames',NULL,'NameMasterAppID','appid','AppID') -- inaccurate? for 48,AlternateNameMasterNames
        ,('Organization',NULL,'NameMasterAppID','appid','AppID')
        ,('Person',NULL,'NameMasterAppID','appid','AppID') -- don't see how this could matter,NameMasterAppID could maybe replace APPID as table key also,maybe,Person
        ,('STUD_AIMS',NULL,'ID_NUM','id_num','APPID') -- UI added ADV_REQ_CODE,STUD_AIMS, was AIM_LABEL,ADV_REQ_CODE
        ,('TEST_SCORES_DETAIL','TST_CDE,TST_SEQ,TST_ELEM','ID_NUM','id_num','APPID') -- UI,TEST_SCORES_DETAIL
        ,('ADVISOR_STUD_TABLE','ADVISOR_ID,DIV_CDE,SEQ_NUM','ID_NUM','id_num','APPID')
        ,('ADV_MTG_NOTES','ADVISOR_ID,MTG_DTE_TIM','ID_NUM','id_num','APPID') -- from UI,ADV_MTG_NOTES
        ,('ADV_MTG_HIST','ADVISOR_ID,MTG_DTE_TIM','ID_NUM','id_num','APPID') -- from UI,ADV_MTG_HIST
        ,('STUD_ADV_ALTER','AIM_LABEL,ADV_TREE_REQ_CDE,SEQ_NUM','ID_NUM','id_num','APPID') -- UI,STUD_ADV_ALTER
        ,('STUD_CRS_NEEDS','ADV_REQ_CODE,CLASS_CDE,REQUIRED_FLAG','ID_NUM','id_num','APPID') -- UI  added CLASS_CDE REQUIRED_FLAG,STUD_CRS_NEEDS
        -- ADV_MTG_NOTES,ADV_MTG_HIST,ADV_MASTER -- ADV_MTG_NOTES,ADV_MTG_HIST,ADV_MASTER
        -- ADVISING_HISTORY,ADV_MASTER -- ADVISING_HISTORY,ADV_MASTER
        --ADVISOR_STUD_TABLE,ADV_MASTER --ADVISOR_STUD_TABLE,ADV_MASTER
        --EX_PROGRAM_GPA_STUDENT_MASTER,ADV_MASTER --EX_PROGRAM_GPA_STUDENT_MASTER,ADV_MASTER
        --STUD_ADV_ALTER,ADV_MASTER --STUD_ADV_ALTER,ADV_MASTER
        -- STUD_AIMS,ADV_MASTER -- STUD_AIMS,ADV_MASTER
        -- STUD_CRS_NEEDS,ADV_MASTER -- STUD_CRS_NEEDS,ADV_MASTER
        -- skipping ADVISOR_STUD_TABLE,ADVISING_GROUP_DEF -- skipping ADVISOR_STUD_TABLE,ADVISING_GROUP_DEF
        -- skipping STUDENT_DIV_MAST,ADVISING_GROUP_DEF -- skipping STUDENT_DIV_MAST,ADVISING_GROUP_DEF
        -- skipping ADVISOR_STUD_TABLE,ADVISOR_MASTER -- skipping ADVISOR_STUD_TABLE,ADVISOR_MASTER
        -- skipping AlternateContactMethod_UDF,AlternateContactMethod -- skipping AlternateContactMethod_UDF,AlternateContactMethod
        -- CM_EMERG_CONTACTS,AlternateContactMethod -- CM_EMERG_CONTACTS,AlternateContactMethod

        ,('ADVISING_ACCESS','LOGIN_ID','ID_NUM','id_num','APPID')
        ,('ALTERNATE_IDENTIFIER','IDENTIFIER_TYPE,BEGIN_DTE,END_DTE','ID_NUM','id_num','APPID') -- was SEQ_NUM changed per UI,ALTERNATE_IDENTIFIER
        ,('CM_EMERG_CONTACTS',NULL,'ID_NUM','id_num','APPID')
        ,('AlternateContactMethod','ADDR_CDE','ID_NUM','id_num','APPID') -- from UI,AlternateContactMethod
        -- EX_REQUIRED_GOVT_FORMS,AP_MASTER -- EX_REQUIRED_GOVT_FORMS,AP_MASTER
        -- TRANS_HIST,INVOICE_HEADER,AP_MASTER -- TRANS_HIST,INVOICE_HEADER,AP_MASTER
        -- INVOICE_HEADER,AP_MASTER,AP_MASTER -- INVOICE_HEADER,AP_MASTER,AP_MASTER

        -- IND_YTD_BEN_HIST,IND_BEN_MAST,AP_MASTER -- IND_YTD_BEN_HIST,IND_BEN_MAST,AP_MASTER
        ,('EX_REQUIRED_GOVT_FORMS','SUBSID_CDE,GOVT_FORM_CODE','ID_NUM','id_num','APPID')
        -- 
        -- HERE -- select top 10 * from TmsEPly..TRANS_HIST   where AP_SBS_ID_NUM  in (4308704, 4314021)

        ,('TRANS_HIST',NULL,'ID_NUM','id_num','APPID') -- UI,TRANS_HIST mess: 'SOURCE_CDE,GROUP_NUM,TRANS_KEY_LINE_NUM'
        ,('INVOICE_HEADER','INVOICE_NUM','ID_NUM','id_num','APPID') -- UI,INVOICE_HEADER
        ,('IND_YTD_BEN_HIST','BENEFIT_CDE,CALENDAR_YR','ID_NUM','id_num','APPID') -- UI,IND_YTD_BEN_HIST
        ,('IND_BEN_MAST','BENEFIT_CDE','ID_NUM','id_num','APPID') -- UI,IND_BEN_MAST
        ,('AP_MASTER','SUBSID_CDE','ID_NUM','id_num','APPID') -- UI,AP_MASTER
        ,('Archive_ADDRESS_HISTORY',NULL,'ID_NUM','id_num','APPID')
        ,('Archive_FEES_HISTORY',NULL,'ID_NUM','id_num','APPID') -- MOST LIKELY todo xkeys= mergeall or new col,Archive_FEES_HISTORY
        ,('Archive_GRADE_MAINT_HIST',NULL,'ID_NUM','id_num','SEQ_NUM') -- MOST LIKELY todo xkeys= mergeall or new col,Archive_GRADE_MAINT_HIST
        ,('ARCHIVE_NAME_HISTORY',NULL,'ID_NUM','id_num','APPID') -- MOST LIKELY todo xkeys= mergeall or new col,ARCHIVE_NAME_HISTORY
        ,('Archive_TW_API_CRP','YR_CDE,TRM_CDE,CRS_CDE','ID_NUM','id_num','APPID')
        ,('ASSET_MASTER','ASSET_NUMBER','ID_NUM','id_num','APPID') -- UI,ASSET_MASTER
        ,('ATTRIBUTE_TRANS','ATTRIB_SEQ','ID_NUM','id_num','APPID') -- UI or maybe ID_NUM,ATTRIBUTE_TRANS
        ,('AVAdviseeRosterClassificationSummary',NULL,'StudentIdNumber','id_num','ROW_ID')
        ,('PF_AWARD','POE_ID,FUND_CDE','ID_NUM','id_num','APPID') -- added APPID per UI,before CANDIDATE,PF_AWARD
        ,('PF_AWARD_TEMP','POE_ID,FUND_CDE','ID_NUM','id_num','APPID') 
        ,('BIOGRAPH_MASTER',NULL,'ID_NUM','id_num','ID_NUM') 
        ,('BIOGRAPH_HISTORY',NULL,'ID_NUM','id_num','APPID') 
        ,('Archive_BIOGRAPH_HISTORY',NULL,'ID_NUM','id_num','APPID')

        ,('CANDIDACY','YR_CDE,TRM_CDE,PROG_CDE,DIV_CDE,LOCA_CDE','ID_NUM','id_num','APPID') -- added APPID per UI before CANDIDATE,CANDIDACY
        ,('REQUIREMENTS','YR_CDE,TRM_CDE,PROG_CDE,DIV_CDE,LOC_CDE,REQ_SEQ','ID_NUM','id_num','APPID')

        ,('STAGE_HISTORY_TRAN',NULL,'ID_NUM','id_num','ID_NUM,HIST_STAGE,TRANSACTION_SEQ') -- maybe before CANDIDACY and CANDIDATE,STAGE_HISTORY_TRAN

        ,('CANDIDATE',NULL,'ID_NUM','id_num','APPID') -- before STAGE_HISTORY_TRAN,CANDIDATE
        --39,STAGE_HISTORY_TRAN,37,CANDIDACY, --39,STAGE_HISTORY_TRAN,37,CANDIDACY,
        --39,STAGE_HISTORY_TRAN,38,CANDIDATE, --39,STAGE_HISTORY_TRAN,38,CANDIDATE,

        --('CM_EMERG_CONTACTS','EMRG_SEQ_NUM,'ID_NUM','ID_NUM','id_num','APPID') -- removed on purpose?,CM_EMERG_CONTACTS
        ,('CHG_CDE','CHG_CDE','ID_NUM','id_num','APPID') -- UI,CHG_CDE
        ,('CHK_HIST_HEADER','SOURCE_CDE,CHECK_BATCH_ID,CHECK_SEQNUM','ID_NUM','id_num','APPID') -- UI,CHK_HIST_HEADER
        ,('CHK_RECONCILIATION','CHECK_NUM_ALPHA,CHECK_NUM_NUMERIC','ID_NUM','id_num','APPID') -- UI,CHK_RECONCILIATION
        ,('COURSE_AUTHORIZATION','YR_CDE,TRM_CDE,CRS_CDE,SEQ_NUM','ID_NUM','id_num','APPID') -- UI,COURSE_AUTHORIZATION
        ,('coursehistory',NULL,'ID_NUM','id_num','APPID')
        --,('CUS_Tmp_FixNaming2',NULL,'ID_NUM','id_num','APPID')
        --,('CUS_TMP_update_logins',NULL,'ID_NUM','id_num','APPID')
        ,('Dataset',NULL,'ID_NUM','id_num','ID_NUM')
        ,('GRADUATION_STAGE','SEQ_NUM_2,GRAD_STAGE_CDE','ID_NUM','id_num','APPID') -- UI,GRADUATION_STAGE
        ,('DEGREE_HISTORY','SEQ_NUM_2','ID_NUM','id_num','APPID') -- todo merge shuffle SEQ_NUM_2 =UNIQ,DEGREE_HISTORY
        ,('DEGREE_HISTORY_ARCHIVE',NULL,'ID_NUM','id_num','APPID') -- MOST LIKELY todo xkeys= mergeall or same shuffle as " or new col,DEGREE_HISTORY_ARCHIVE
        ,('EMERG_CONTACT_MAST','EMER_CON_SEQ','ID_NUM','id_num','ID_NUM,EMER_CON_SEQ')
        ,('EMPL_ACA_COVERED_INDIVIDUALS','SEQ_NUM','EMPL_ID_NUM','id_num','APPID')
        ,('EMPL_MAST_UDF',NULL,'ID_NUM','id_num','APPID')
        ,('EMPL_YTD_ACA_HIST','CALENDAR_YR','ID_NUM','id_num','APPID')
        ,('IND_YTD_DIR','DDP_GRP,DDP_SEQ,CALENDAR_YR','ID_NUM','id_num','ID_NUM,DDP_GRP,DDP_SEQ,CALENDAR_YR')
        ,('IND_DIR_DEP','DDP_GRP,DDP_SEQ','ID_NUM','id_num','APPID') -- UI,IND_DIR_DEP
        ,('IND_SAL_HIST','ORG_POS,POS_SEQ,SAL_RVW_SEQ','ID_NUM','id_num','APPID') -- UI,IND_SAL_HIST
        -- 83,TIMCRDS,60,IND_POS_HIST,65,EMPL_MAST -- 83,TIMCRDS,60,IND_POS_HIST,65,EMPL_MAST
        -- TIMCRDS|IND_POS_HIST|EMPL_MAST -- TIMCRDS|IND_POS_HIST|EMPL_MAST
        ,('TIMCRDS','SOURCE_CDE,TIME_BATCH_ID,TIME_SEQNUM','ID_NUM','id_num','APPID') -- UI,TIMCRDS
        ,('IND_POS_HIST','ORG_POS,POS_SEQ','ID_NUM','id_num','APPID') -- UI,IND_POS_HIST
        ,('IND_YTD_TAX','TAX_CDE,CALENDAR_YR','ID_NUM','id_num','APPID') -- UI,IND_YTD_TAX
        ,('IND_TAX_MAST','TAX_CDE','ID_NUM','id_num','APPID') -- UI,IND_TAX_MAST
        ,('IND_YTD_OTH','CALENDAR_YR','ID_NUM','id_num','APPID') -- UI,IND_YTD_OTH
        ,('SUPERVIS_MAST',NULL,'ID_NUM','id_num','APPID')
        ,('EMPL_MAST',NULL,'ID_NUM','id_num','APPID')
        ,('ETHNIC_RACE_REPORT','SEQ_NUM','ID_NUM','id_num','APPID') -- UI,ETHNIC_RACE_REPORT-- TRIGGER to update _DTL
        ,('ETHNIC_REPORT_DTL','ETHNIC_RPT_DEF_NUM,SEQ_NUM','ID_NUM','id_num','APPID') -- UI,ETHNIC_REPORT_DTL
        ,('RACE_REPORT_DTL','SEQ_NUM,RACE_RPT_DEF_NUM','ID_NUM','id_num','APPID') -- UI,RACE_REPORT_DTL
        ,('FACULTY_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('FEES','SEQUENCE_NUMBER','ID_NUM','id_num','APPID') -- UI,FEES, SEQUENCE_NUMBER - > Shuffle every time
        ,('FEES_HISTORY',NULL,'ID_NUM','id_num','APPID') -- UI,FEES_HISTORY -- BATCH_NUMBER+SEQ_NUM are system wide uniq, but updating ID ok
        ,('FERPA_PERMISSION',NULL,'ID_NUM','id_num','APPID') -- UI,FERPA_PERMISSION
        ,('GF1098TExcludedRecords',NULL,'ID_NUM','id_num','AppID') 
        ,('GF1098TIncludedRecords',NULL,'ID_NUM','id_num','AppID')
        ,('GF1098TIssues',NULL,'ID_NUM','id_num','AppID') -- UI,GF1098TIssues
        ,('GF1098TSummary',NULL,'ID_NUM','id_num','AppID')
        ,('GRADE_MAINT_HIST',NULL,'ID_NUM','id_num','SEQ_NUM') 
        ,('HIGHEST_TEST_SCORE','TST_CDE,TST_ELEMENT','ID_NUM','id_num','APPID') 
        ,('HOLD_TRAN',NULL,'ID_NUM','id_num','APPID') 
        ,('IND_BEN_ACCRUAL_HIST',NULL,'ID_NUM','id_num','APPID') -- UI,was GROUP_NUMBER,ACTION_CODE,IND_BEN_ACCRUAL_HIST
        ,('IND_BEN_HIST','BENEFIT_CDE,IND_BENEFIT_SEQ','ID_NUM','id_num','APPID') 
        ,('IND_PAY_ACC_RATE','ORG_POS,POS_SEQ,SAL_RVW_SEQ,PAY_SEQ','ID_NUM','id_num','APPID') -- UI,IND_PAY_ACC_RATE
        ,('IPEDS_STUDENT_MAST','SNAPSHOT_DTE','ID_NUM','id_num','APPID')
        ,('IR_IPEDS_COMPLETIONS','SNAPSHOT_ID,SEQ_NUM','ID_NUM','id_num','APPID')
        ,('IR_STUDENT_DATA_UDF','SNAPSHOT_ID','ID_NUM','id_num','APPID')
        ,('IR_STUDENT_DATA','SNAPSHOT_ID','ID_NUM','id_num','APPID')
        ,('ITEMS',NULL,'ID_NUMBER','id_num','APPID') -- UI was GROUP_NUMBER,ACTION_CODE,ITEMS, was ID_NUMBER,GROUP_NUMBER,SUBGROUP_NUMBER,GROUP_SEQUENCE
        ,('J1FormattedNames',NULL,'ID_NUM','id_num','AppID') -- UI,J1FormattedNames, was PartyAppID
        ,('LOCATION_MASTER',NULL,'CONTACT_ID_NUM','id_num','APPID') -- pretty sure 3/1/22
        --,('MCN_COHORT2009',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_COHORT2010',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_COHORT2011',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_Connect_ACL',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_DataAutomationLog',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_Dataset',NULL,'ID_NUM','id_num','IncorrectAPPID') -- UI,MCN_Dataset
        ,('MCN_REDFIELD_RESULTS',NULL,'ID_NUM','id_num','ID_NUM,RULE_NUM')
        --,('MCN_RESULTS',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_RESULTS_12',NULL,'ID_NUM','id_num','IncorrectAPPID')
        ,('MEDIA_WRK','REC_TYPE,SEQ','ID_NUM','id_num','ID_NUM,REC_TYPE,SEQ') -- prob error ignored,MEDIA_WRK
        ,('MILITARY_SERVICE_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('NAMEPHONEMASTER',NULL,'NameMasterAppID','appid','AppID') -- first attmept at APPID,NAMEPHONEMASTER
        ,('TRANSCRIPT_REQUEST','SEQ_NUM_2','ID_NUM','id_num','APPID') -- UI,TRANSCRIPT_REQUEST
        ,('NameAddressMaster','ADDR_CDE,AddressCodeDefAppID','ID_NUM','id_num','AppID') -- fixed?,NameAddressMaster
        ,('ADDRESS_HISTORY',NULL,'ID_NUM','id_num','APPID') -- todo xkeys= mergeall or new col REQUIRED_FLAG,ADDRESS_HISTORY
        ,('NEW_MERGE_ID_FILE',NULL,'ID_NUM','id_num','PROCESS_ID,LETTER_CDE,ID_NUM,GROUP_NUMBER,SUBGROUP_NUMBER,GROUP_SEQUENCE')
        ,('NSLC_EXCEPTIONS',NULL,'ID_NUM','id_num','APPID')
        ,('NSLC_REG_TRANSACT','NSL_YR_CDE,NSL_TRM_CDE,NSL_SEQ_NUM','NSL_ID_NUM','id_num','APPID') --UI mentions NSL_NSL_YR_CDE prob error ignored,NSLC_REG_TRANSACT
        ,('NSLC_STUDENT_MAST',NULL,'NSL_ID_NUM','id_num','NSL_ID_NUM')
        ,('NSLC_TRANS_HISTORY','NSL_FICE_CDE,NSL_BRANCH_CDE,NSL_ACAD_TERM','NSL_ID_NUM','id_num','NSL_ID_NUM')-- can't serialize dates, NSL_ACAD_TERM should be NSL_REPORT_DTE
        ,('ONLINE_PAYMENT',NULL,'ID_NUM','id_num','APPID')
        ,('ORG_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('PA_WRK_STDY_EDIT',NULL,'ID_NUM','id_num','APPID') -- todo xkeys= mergeall or new col,PA_WRK_STDY_EDIT
        ,('PF_APPLICANT_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('PF_DIRECT_LOAN_RPT',NULL,'ID_NUM','id_num','APPID')
        ,('PF_DISB_TRANS_HIST',NULL,'ID_NUM','id_num','SEQ_NUM')
        ,('PF_ERROR_LOG',NULL,'ID_NUM','id_num','APPID')
        ,('PF_SAP_HIST','YR_CDE,TRM_CDE','ID_NUM','id_num','APPID') -- UI,PF_SAP_HIST
        ,('PF_STDNT_AWARD','POE_ID,FUND_CDE,RELEASE_STS','ID_NUM','id_num','APPID')
        ,('PF_STDNT_EMP','FUND_CDE,POE_ID,WRK_STDY_DEPT','ID_NUM','id_num','APPID')
        ,('PF_STDNT_MSTR','POE_ID','ID_NUM','id_num','APPID') -- UI,PF_STDNT_MSTR
        ,('PF_WRK_STDY','POE_ID,FUND_CDE,WRK_STDY_DEPT','ID_NUM','id_num','APPID')
        ,('PO_HEADER','GRP_NUM','ID_NUM','id_num','APPID') -- UI,PO_HEADER
        ,('Query',NULL,'ID_NUM','id_num','APPID')
        ,('RECEIPT_HEADER',NULL,'ID_NUM','id_num','APPID') -- UI-- really?,RECEIPT_HEADER
        ,('RELATION_TABLE','REL_TABLE_SEQ','ID_NUM','id_num','APPID') -- UI,RELATION_TABLE
        ,('REQ_HEADER','GRP_NUM','ID_NUM','id_num','APPID') -- UI,REQ_HEADER
        ,('SEVIS_STU_FINANCIAL',NULL,'ID_NUM','id_num','APPID') -- STUD_SEQ_NUM,BATCH_NUMBER,SEVIS_STU_FINANCIAL
        ,('SEVIS_STUDENT',NULL,'ID_NUM','id_num','APPID')
        ,('SEVIS_GENERAL',NULL,'ID_NUM','id_num','APPID')
        ,('SPORTS_TRACKING','YR_CDE,TRM_CDE,SPORTS_CDE','ID_NUM','id_num','APPID') -- UI,SPORTS_TRACKING
        ,('STUD_AIMS_TEMP','AIM_LABEL,ADV_REQ_CODE','ID_NUM','id_num','APPID')
        ,('STUD_HOURS_CRS',NULL,'ID_NUM','id_num','APPID') -- 'YR,TRM' 3/31/22 removed since recalc will fix
        ,('STUD_STERM_SUM_DIV',NULL,'ID_NUM','id_num','ID_NUM,DIV_CDE,YR_CDE,TRM_CDE,SUBTERM_CDE') -- 'DIV_CDE,YR_CDE,TRM_CDE,SUBTERM_CDE' 3/31/22 removed since recalc will fix
        ,('STUD_TERM_SUM_DIV',NULL,'ID_NUM','id_num','APPID') -- UI,STUD_TERM_SUM_DIV, 'DIV_CDE,YR_CDE,TRM_CDE' 3/31/22 removed since recalc will fix 
        ,('STUDENT_CRS_HIST_PFLAG_HIST',NULL,'ID_NUM','id_num','APPID') -- UI,but?,STUD_SEQ_NUM,BATCH_NUMBER,STUDENT_CRS_HIST_PFLAG_HIST, 'YR_CDE,TRM_CDE'  3/31/22 removed since recalc will fix
        ,('STUDENT_CRS_HIST','YR_CDE,TRM_CDE','ID_NUM','id_num','APPID') -- UI ?,STUD_SEQ_NUM,STUDENT_CRS_HIST
        --('student_crs_hist_20151026_bkup',NULL,'ID_NUM','id_num','APPID'),student_crs_hist_20151026_bkup
        ,('STUDENT_DIV_MAST','DIV_CDE','ID_NUM','id_num','APPID') -- UI,STUDENT_DIV_MAST
        ,('STUDENT_REG_ALLOWED_LOC','LOC_CDE','ID_NUM','id_num','APPID')
        ,('STUDENT_MASTER_EXT',NULL,'ID_NUM','id_num','APPID')
        ,('STUDENT_MIDTRM_SUM',NULL,'ID_NUM','id_num','ID_NUM,DIV_CDE,YR_CDE,TRM_CDE') -- 'DIV_CDE,YR_CDE,TRM_CDE' 3/31/22 removed since recalc will fix 
        ,('STUDENT_TERM_SUM',NULL,'ID_NUM','id_num','APPID') -- UI,STUDENT_TERM_SUM, 'YR_CDE,TRM_CDE' 3/31/22 removed since recalc will fix 
        ,('STUDENT_TERM_TABLE','YR_CDE,TRM_CDE','ID_NUM','id_num','APPID') -- UI,STUDENT_TERM_TABLE
        ,('TRANSCRIPT_NOTE','DIV_CDE,YR_CDE,TRM_CDE,SEQ_NUM_2','ID_NUM','id_num','APPID')
        ,('TRANSCRIPT_HEADER',NULL,'ID_NUM','id_num','ID_NUM,DIV_CDE,YR_CDE,TRM_CDE')
        ,('STUDENT_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('STUDENT_PROGRESS','AIM_LABEL,ADV_TREE_REQ_CDE,SEQ_NUM','ID_NUM','id_num','APPID') -- UI,STUDENT_PROGRESS
        ,('STUDENT_PROGRESS_TEMP','AIM_LABEL,ADV_TREE_REQ_CDE,SEQ_NUM','ID_NUM','id_num','APPID') -- UI,STUDENT_PROGRESS_TEMP
        ,('STUDENT_YR_REPORT','YR','ID_NUM','id_num','APPID') -- UI,STUDENT_YR_REPORT
        ,('SUBMISSION',NULL,'SUBMITTER_ID','id_num','APPID')
        ,('SUBMISSION_HISTORY','SEQ_NUM','ID_NUM','id_num','APPID')
        ,('SUBSID_MASTER','SUBSID_CDE','ID_NUM','id_num','APPID') -- UI,SUBSID_MASTER
        ,('SUBSID_RPT','SUBSID_CDE,LOCK_JOB_NUM,USER_NAME','ID_NUM','id_num','APPID') -- UI,SUBSID_RPT
        ,('TEST_SCORES_UDF','TST_CDE,TST_SEQ','ID_NUM','id_num','APPID')
        ,('TEST_SCORES','TST_CDE,TST_SEQ','ID_NUM','id_num','APPID') -- UI,TEST_SCORES
        ,('trans_temp',NULL,'ID_NUM','id_num','APPID')
        ,('TW_API_CRP','YR_CDE,TRM_CDE,CRS_CDE','ID_NUM','id_num','ID_NUM,YR_CDE,TRM_CDE,CRS_CDE')
        ,('TW_API_CST','JICS_GROUP_ID','ID_NUM','id_num','ID_NUM,JICS_GROUP_ID')
        ,('TW_API_PRS',NULL,'ID_NUM','id_num','ID_NUM')
        ,('TW_GRP_MEMBERSHIP','GROUP_ID','ID_NUM','id_num','ID_NUM,GROUP_ID')
        ,('TW_WEB_SECURITY',NULL,'ID_NUM','id_num','AppID') -- UI,TW_WEB_SECURITY
        ,('VENDOR_WEB_SITE','URL','ID_NUM','id_num','APPID')
        ,('VNDR_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('NameMaster',NULL,'ID_NUM','id_num','APPID')
        ,('NAME_HISTORY',NULL,'ID_NUM','id_num','APPID')
        -- sp_who2

        update DEK 
        set tableuniqkey = replace(P.cols,' ','') 
        from {tbl} DEK
        join {con} P
        on P.tablename = DEK.tablename

        update {tbl} --somehow this is actually really correct
        set tableuniqkey = 'APPID'
        where tablename = 'CANDIDACY'  

        update {tbl}
        set defaultaction = 'shuffle'
        where xkeys like '%seq%' and tablename in ( -- not quite making sure seqWhatever is the last thing in the list, but we'll call it good
        'ETHNIC_RACE_REPORT'
        , 'NSLC_REG_TRANSACT'
        , 'DEGREE_HISTORY'
        )
        """.format(tbl=self.tblxkeys, con=self.tblPKConstraints )
        self.db.execute_i_u_d(sql)

###########################
#the below is a manual test.
if __name__ == '__main__':
    # dupset = 2
    sumpin = Dupset(2)
    # sumpin._rebuildupextrakeys() # run this manually if you update anything in that def

    print(sumpin.ids)
    print(sumpin.dupset)
    print(sumpin.jdbname)
    print(sumpin.goodid)
    print(sumpin.status)
    # print(sumpin.update_status())
    # print("status: {}".format(sumpin.status))
    # sumpin.update_formdata()
    # print("status: {}".format(sumpin.status))


    # sql = "SELECT 100/20"
    # sql = "SELECT 1/0"
    # r="go"
    # r = sumpin.db.execute_i_u_d(sql)
    
    # print ("r={}".format(r))
    # if 'error' in r and r['error']:
    #     patternref = re.compile('REFERENCE constraint[^"]*"([^"]*)".*table[^"]*"([^"]*)"')
    #     patterndupinsert = re.compile("Cannot insert duplicate key row[^']*'([^']*)'.*index[^']*'([^']*)'")
    #     patternconflict = re.compile('UPDATE statement conflicted with the FOREIGN KEY constraint "([^"]*)".*table "([^"]*)') 
    #     patternpkviolation = re.compile("Violation of PRIMARY KEY constraint '([^']*)'.*object '([^']*)'.*value is ([^)]*)")
    #     # only put this in the except part
    #     e = r['error'].args[1]
    #     ans = patternref.findall(e)
    #     if ans:
    #         for a in ans:
    #             print("patternref:{}".format(a))
    #     else:
    #         print("no patternref")
    #     ans = patterndupinsert.findall(e)
    #     if ans:
    #         for a in ans:
    #             print("patterndupinsert:{}".format(a))
    #     else:
    #         print("no patterndupinsert")

    #     ans = patternconflict.findall(e)
    #     if ans:
    #         for a in ans:
    #             print("patternconflict:{}".format(a))
    #     else:
    #         print("no patternconflict")

    #     ans = patternpkviolation.findall(e)
    #     if ans:
    #         for a in ans:
    #             print("patternpkviolation:{}".format(a))
    #     else:
    #         print("no patternpkviolation")
            
    # print ('r:{}'.format(r))
    # print ('ErrorNum:{}'.format(r['error'].args[0]))
    # print ('ErrorExp:{}'.format(r['error'].args[1]))
    print("formheaderinfo: {}".format(sumpin.formheaderinfo))
    print("formbodyinfo: {}".format(sumpin.formbodyinfo))
    # # print("sqlinfo[guts]: {}".format(sumpin.sqlinfo['guts']))
    print("sqlinfo: {}".format(sumpin.sqlinfo))

    # print("ids in namemaster: {}".format(sumpin._idsintable('NameMaster')))

#  this might only be needed when running from the command line, testing
from os import replace
import sys
import json
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
        self.ids = {}
        self.goodid = 0
        self.goodappid = 0
        # self.goodgoid = 0
        self.status = "" # started, needs keys, ready, staged?, complete?
        self.formheaderinfo = {}
        self.formbodyinfo = []
        self.sqlinfo = {}

        self.db = pyodbc_db.MSSQL_DB_Conn() # maybe this is the way to only use one db connection?
        sql = """select distinct db from {}..BAY_DupIDs""".format(dbname)
        cnt = 0
        try:
            r = self.db.execute_s(sql)
            cnt = self.db.record_count 
        except Exception as e:
            print("exception {}".format(e))
        if cnt != 1:
            self.error.append("Something is wrong, please rebuild the dup list, exactly 1 databases required.")
        else:
            self.jdbname = r[0]['db']
        
        """ id_num, goodid for each in a given dupset"""
        sql = """
        select 
        D.goodid as id_num
        , goodid
        , NameMasterAppID as appid
        , PartyAppID
        , PartyGOID as goid
        from {}..BAY_DupIDs D
        join {}..NameMasterPersonOrgPartyView PV
        on PV.ID_NUM = D.goodid
        where dupset = {} and  isnull(goodid,0)>0
        union
        select 
        D.id_num	
        , goodid
        , isnull(NameMasterAppID,59476) as appid -- dupset 84 fix
        , PartyAppID
        , PartyGOID as goid        
        from {}..BAY_DupIDs D
        left join {}..NameMasterPersonOrgPartyView PV
        on PV.ID_NUM = D.id_num        
        where dupset = {}    
        """.format(dbname, self.jdbname, dupset, dbname, self.jdbname, dupset)
        dupids = self.db.execute_s(sql)
        # print('dupids {}'.format(dupids))
        if dupids and not dupids[0]['goodid']:
            self.error.append("Please identify the goodid and refresh duplist first (dupset {})".format(dupset))
        elif dupids:
            for i in range(len(dupids)):
                if dupids[i]['id_num'] == dupids[i]['goodid']:
                    self.goodid = dupids[i]['goodid']
                    self.goodappid = dupids[i]['appid']
                    # self.goodgoid = dupids[0]['goid'] # etc
            # sort the ids to put goodid first, then ohen others in sorted order, for repeatability later
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

        else:
            self.error.append("dupset {} had nothing to look at".format(dupset))

    def update_status(self):
        # self.status = not prepped, needs keys, ready, staged?, complete?

        # 1. dupset is inserted into BAY_DupsInProgress noting any that that need more info via HELPME flag in xkey col
        # 2. Work through each HELPME adding new BAY_DupExtraKeys values and trying again
        # 3. all HELPME records gone, move on to full merge set of tables
    
        # this SQL determines if a dupset is being worked on, and ready for final processing

        helpme ='{"HELPME": "HELPME"}' # needs overhaul TODO, ofr now notdone==0
        sql = """select  dupset, count(*) as cnt
            , max(case isnull(xkeys,'') when '{}' then 1 else 0 end) as notdone
            from {}..BAY_DupsInProgress
            where dupset = {}
            group by dupset
        """.format(helpme, self.dbname, self.dupset)
        ckstatus = self.db.execute_s(sql)
        # print("ckstatus={}".format(ckstatus))

        if ckstatus and ckstatus[0]['notdone']:
            self.status = "needs keys"
        elif ckstatus:
            self.status = "ready"
        else:
            self.status = "not prepped"
        return self.status
    
    def _listalltables(self):
        '''return list of all tables that might need fixing'''
        # ID	tablename	xkeys	tablekeyname	mykeyname
        # 2470	AD_ORG_TRACKING	SEQ_NUM	ID_NUM	id_num
        sql = "SELECT * from {}..BAY_DupExtraKeys order by id".format(self.dbname)
        return self.db.execute_s(sql)

    def _idsintable(self, tabledict, ek={}):
        # return list the id_num count, and any other fields that are required for key2 generation
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
        return ['APPROWVERSION','CHANGEUSER','CHANGEJOB','CHANGETIME','USER_NAME','JOB_TIME','LAST_UPDATE','JOB_NAME']

    def _insertintodiptable(self, tabledict, ek={}):
        ''' insert into BAY_DupsInProgress'''
        newek = []
        table = tabledict['tablename']
        tkey =  tabledict['tablekeyname']
        mykey = tabledict['mykeyname']
        cnt = tabledict['fullcount']
        approwversion = ""
        sek = json.dumps(ek)
        sql = """insert into {}..BAY_DupsInProgress
        (dupset, tablename, tablekeyname, mykeyname, xkeys, db, cnt) VALUES ({},'{}','{}','{}','{}','{}','{}')
        """.format(self.dbname, self.dupset, table, tkey, mykey, sek, self.jdbname, cnt)
        self.db.execute_i_u_d(sql)

    def _getDupsInProgressList(self):
        """ return list of only this dupset from prep table with good set of data"""
        # top 5 for quick way to only look at a few things
        sql = """select  DIP.*, DEK.tableuniqkey from {}..BAY_DupsInProgress DIP
            join {}..BAY_DupExtraKeys DEK on DIP.tablename = DEK.tablename
            where dupset = {} order by DIP.ID
            """.format(self.dbname, self.dbname, self.dupset)
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
        parts = cnt.split(', ')
        intparts = [int(p) for p in parts]
        p1 = intparts[0]
        p2 = sum(intparts[1:])
        return p1, p2

    def _catagorizecnt(self, cnt):
        ''' categories of cnt and what to do with them:
        0,1...	 BCD
        1...,0	-
        1,1	    ABCD
        1,2...   BCD (maybe a, but too much work for nada)
        2,1...	 BCD
        
        A = show boxes and update T0
        B = update all bad id records with good id
        C = insert bad record(s) into new ones with good id
        D = delete all bad ID records
        '''
        p1,p2 = self._cntparts(cnt)
        if p1 == 0 and p2 >= 1:
            return "BCD"
        elif p1 >= 1 and p2 == 0:
            return ""
        elif p1 == 1 and p2 == 1:
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
            <button type="button" onclick="copybutton(this)" onmouseout="renamebutton(this)">Copy text</button>
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
insert into {}..{} ({}, {})
select {}, {}
from {}..{} where {} in ({}) {}""".format(self.jdbname, table, ttkeys, fieldnames,
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
                p1,p2 = self._cntparts(cnt)
                details = []
                if p1 > 0:
                    details.extend(self._detailsfromTableAndxKeys(tkey, goodid, table, xkeys))
                if p2 > 0:
                    details.extend(self._detailsfromTableAndxKeys(tkey, allbutgoodid, table, xkeys))
                cids.append(details)

                foundationsql.append("-- INFO: This is the SQL to base decisions on, count={}".format(cnt))
                axkeys = self._axkeys(xkeys)
                foundationsql.append("select * from {}..{} where {} in ({}) {}".format(self.jdbname, table, tkey, ids, axkeys))

                index = 0
                tdels.append("-- D: usually used after a good update")
                tdels.append("select * -- delete \nfrom {}..{} where {} in ({})".format(self.jdbname,
                    table, tkey, allbutgoodid))
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
                        t = """-- B: update all bad ids to good ids, hey it works sometimes
update {}..{} set {}={}{} where {} in ({}) {} """.format(self.jdbname, table,
                            tkey, goodid, xset, tkey, allbutgoodid, axkeys)
                        torsB.append(t)

                        tjoins.append('-- A: Update based on table selections below\n<span class="guts"></span> from {}..{} T0 '.format(self.jdbname, table))
                            # torsB.append("--item {} =?= {} with key {}".format(item, self.ids, tkey.lower()))
                    else:
                        tjoins.append("join {}..{} T{} on {}".format(self.jdbname, table, index, item))
                        # need to triple check this works with one bad id
                tjoins.append("where {} ".format(T[0])) # T0

                ans = []

                if A: # must go first for javascript button and guts to work correctly
                    ans.extend(self._wrapsqlwithcopybutton(tjoins)) # guts are in here somewhere
                
                ans.extend(self._wrapsqlwithcopybutton(foundationsql))
                ans.extend(self._wrapsqlwithcopybutton(tapprowverck))
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

        # 1. dupset is inserted into BAY_DupsInProgress noting any that that need more info via HELPME flag in xkey col
        # 2. Work through each HELPME adding new BAY_DupExtraKeys values and trying again
        # 3. all HELPME records gone, move on to full merge set of tables

        # initialize self.status if not set: "needs keys" |  "ready" | "not prepped"
        if not self.status:
            self.update_status()

        if self.status == "not prepped":
            # nothing for this dupset, let's fill the BAY_DupsInProgress best we can
            # then check it again
            tablelist = self._listalltables()
            # tablelist = [{'ID': 2624, 'tablekeyname':'ID_NUM', 'mykeyname':'id_num', 'tablename': 'TW_GRP_MEMBERSHIP', 'xkeys': 'GROUP_ID', 'ID_NUM': 'ID_NUM', 'APPID': None}]
            for t in tablelist:
                table =  t['tablename']
                # tablekeyname	mykeyname
                # ID_NUM	id_num
                xkeys = t['xkeys']
                tkey =  t['tablekeyname']
                mykey = t['mykeyname']
                ids = ", ".join(map(str, self.ids[mykey]))

                # ck = self.db.columns
                cols = self._colsfromtable(table)
                extrakeys = self._dupextrakeysuniqvaluekeys(t)
                # extrakeys = [{'YR':2017, 'TRM':10}, {'YR':2017, 'TRM':20}, {'YR':2017, 'TRM':30}]
                helpme = ""
                # try:
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
            self.update_status()

        # now self.status should be either "needs keys" or  "ready
        if self.status == "needs keys":
            # not all ready, define some more keys
            # just work with the notdone flagged ones
            # tablelist = self._allnotdonetablesfordupset() 
            # finish this here for HELPME flagged ones, probably filling default xkeys on this query
            
            # stub for now, just drop HELPME  rows
            tablelist = self._getDupsInProgressList()
            self._loopoverdupsinprogress(tablelist)

        elif self.status == "ready":
            # must all be OK, let's do it for real
            tablelist = self._getDupsInProgressList()
            # tablelist = [{'ID':4293, 'db':'TmsEPly',
            # 'dupset':'TmsEPly',
            # 'tablename':'ARCHIVE_NAME_HISTORY',
            # 'tablekeyname':'ID_NUM',
            # 'mykeyname'	:'id_num',
            # 'xkeys':'',
            # 'APPROWVERSION':None }]
            self._loopoverdupsinprogress(tablelist)
        else:
            # very odd
            print("error 511, eject, something is very wrong, status={}".format(self.status))

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
    print("status: {}".format(sumpin.status))
    sumpin.update_formdata()
    print("status: {}".format(sumpin.status))

    # print("formaheaderinfo: {}".format(sumpin.formheaderinfo))
    print("formbodyinfo: {}".format(sumpin.formbodyinfo))
    # print("sqlinfo[guts]: {}".format(sumpin.sqlinfo['guts']))
    print("sqlinfo: {}".format(sumpin.sqlinfo))

    # print("ids in namemaster: {}".format(sumpin._idsintable('NameMaster')))

#!/var/www/flaskr/venv/bin/python
#  this might only be needed when running from the command line, testing
import sys
sys.path.insert(0, r'/var/www/flaskr')
sys.path.insert(0, r'/var/www/flaskr/flaskr')
from flaskr import pyodbcRISD as dbc
# from flaskr import pyodbc_db_assessment as dbsourcea
# from flaskr import pyodbc_db_assessment as dbsourceb
from myflaskrsecrets import dbserverassessment, dbnameassessment, dbuidassessment, dbpwdassessment

# import datetime
import io
from datetime import datetime
import re

class MyDeletionForms():
    def __init__(self, username="", includeinactive=False):
        '''A set of deletion forms that belong, or have some relation to this user'''
        self.username = username.lower()
        self.includeinactive = includeinactive
        self.forms = []
        if self.username == "":
            pass
        elif self.username > "" and "'" not in self.username: # very sloppy "protection"
            self.dba = dbc.DBConnect()
            sqlincludeinactive = "active = 1 and"
            if self.includeinactive:
                sqlincludeinactive = ""
            sql = f"select id from invdelform where {sqlincludeinactive} username = ?"
            ans = self.dba.execute_s(sql, self.username)
            # note if answer is empty??
            for a in ans:
                self.forms.append(DeletionForm(self.username, id=a['id']))
            # now check auth chain for those I need to see, and append them to this list TODO
        else:
            pass

    def __str__ (self):
        a2 = ""
        fc = len(self.forms)
        if self.includeinactive:
            if fc == 1:
                a2 = ", an inactive one"
            else:
                a2 = "s including inactive ones"
        elif fc != 1:
            a2 = "s"

        return f"MyDeletionForms({self.username}) -- {fc} form{a2}"

class DeletionForm():
    def __init__(self, username="", id=0, school="", schoolunitdeltingitems="", wonumber="", notes=""):
        '''A deletion form has attributes on the form, and line items with different type attributes.
        It also has an editing process and an approval process '''
        # default these values for new entry, then overwrite with query if id exists
        self.id = id # should be a passed number or default=0="new"
        self.wonumber = wonumber
        self.schoolunitdeltingitems = schoolunitdeltingitems
        self.school = school
        self.active = 1 # ok?
        self.notes = notes
        self.username = username
        self.updated = datetime.now().strftime("%x")
        self.lines = []
        self.linecount = 0
        self.dba = dbc.DBConnect()
        self.dbv = dbc.DBConnect(db="visions")

        # self.dbf = get_db() # forms
        if self.id > 0:
            self._readheaderinfo()
            # self.fillinformdetails() # maybe don't preload these (performance), but len(self.lines) used in __str__
        elif self.username != "" and self.id == 0:
            self.id = self._createnew()
        else: # I think you can't get here
            return('Username is required on new deletion form creation.') # better way to deliver this news?

    def __str__ (self):
        return f"DelForm({self.id}) by {self.username} with {len(self.lines)} line(s) -- Notes: {self.notes}"

    def _createnew(self):
        ''' create a new blank deletion form, add what you know to it '''
        # self.linecount = 0
        sql = """insert into invdelform
            (school, schooldeleting, workorder, notes, username, active, updated)
            OUTPUT Inserted.id
            values (?,?,?,?,?,?,?)""" # really need to protect this better
        self.id = self.dba.execute_i_u_d(sql, (self.school, \
            self.schoolunitdeltingitems, self.wonumber, self.notes,\
            self.username, self.active, self.updated))

    def _readheaderinfo(self):
        if not self.id:
            return []
        try:
            # sql = "select *,(select count(distinct tag) from invdelformdetails where invid=F.id) as tagcount from invdelform F where id = ?;"
            sql = """select top 1 * 
                , (select count(*) from invdelformdetails where invid=F.id ) as linecount
                from invdelform F
                where F.id =  ?"""
            rec = self.dba.execute_s(sql, self.id)
            self.wonumber = rec[0]['wonumber']
            self.schoolunitdeltingitems = rec[0]['schoolunitdeltingitems']
            self.school = rec[0]['school']
            self.active = rec[0]['active'] # False vs 0, fail
            # print(f"HEY208, self.active={self.active}")
            self.notes = rec[0]['notes']
            self.username = rec[0]['username']
            self.updated = rec[0]['updated']
            self.linecount = rec[0]['linecount']
        except Exception as e:
            return ['_readheaderinfo failed', e, sql]

    def _updateheaderrecord(self):
        # put it exactly as sent in f, blank out any fields that are blank
        try:
            # id =  f['id']
            # del f['id']
            # (keys,values) = zip(*f.items())
            # updatevals = list(values)
            # updateme = ['{} = ?'.format(key) for key in keys]
            # cur = db.cursor()
            # if updateme:
            #     sql = "update invdelform set {}, updated=CURRENT_TIMESTAMP where id = ?;".format(', '.join(updateme))
            #     updatevals.append(id)
            #     cur.execute(sql, (updatevals))
            #     db.commit()
            #     cur.close # ?
            #     return True
            # else:
            #     return ['Nothing to do with id {}'.format(id)]
            sql = ""
            # print("HEY157, no F yet")
            f = {'schoolunitdeltingitems':self.schoolunitdeltingitems, 'wonumber':self.wonumber, 'notes':self.notes, 'username':self.username, 'active':self.active }
            # print(f"HEY161 f={f}")
            (keys,values) = zip(*f.items())
            # print(f"HEY163 keys={keys}, values={values}")
            updatevals = list(values)
            # print(f"HEY165 updatevals={updatevals}")
            updatevals.append(self.id)
            # print(f"HEY167 updatevals={updatevals}")
            updateme = ['{} = ?'.format(key) for key in keys]
            # print(f"HEY169 updateme={updateme}")

            sql = "update invdelform set {}, updated=CURRENT_TIMESTAMP where id = ?;".format(', '.join(updateme))
            # print(f"HEY172, sql={sql}")
            self.dba.execute_i_u_d(sql, tuple(updatevals))

        except Exception as e:
            return [e, sql]

    def _inactivateform(self):
        '''just flag it as inactive'''
        try:            
            sql = "update invdelform set active=0, updated=getdate() where id = ?;"
            # print(f"HEY172, sql={sql}")
            self.dba.execute_i_u_d(sql, self.id)

        except Exception as e:
            return [e, sql]

    def fillinformdetails(self):
        try:
            sql = "select * from invdelformdetails where invid = ?"
            rec = self.dba.execute_s(sql, self.id)
            # print(f"HEY98, sql={sql} self.id={self.id} rec={rec}")
            for a in rec:
                # print(f'HEY99 adding line {a["id"]} to this form {self.id}')
                self.lines.append(a)
            self.linecount = len(self.lines)
        except Exception as e:
            return [e, sql]

    def insertdetailrecordsfromtaglist(self, taglist):
        '''loop over [taglist] and pick best description for each tag, returning dictionary of tags and descriptions'''
        sql = "select * from NOTdetails where tagnum in (?)"

    def insertdetailrecord(self, values):
        '''Inserts one new detail record into the DB for this form, using {values}.
            Does not update form line details, but does bump up count by one.'''
        try:
            # invid = id of form
            columns = ', '.join(values.keys())
            placeholders = ', '.join('?' * len(values))
            sql = '''INSERT INTO invdelformdetails ({}) 
                -- OUTPUT Inserted.id 
                VALUES ({})'''.format(columns, placeholders)
            self.dba.cursor.execute_i_u_d(sql, values.values())
            self.linecount += 1
        except Exception as e:
            return [e, sql]

    def _deletedetailrecord(dbf, id): # TODO
        try:
            cur = dbf.cursor()
            sql = 'delete from invdelformdetails where id=?'
            cur.execute(sql, (id)) # tuple(id) fails here, not sure why
            dbf.commit()
            cur.close # ? good idea?
            return True
        except Exception as e:
            return [e, sql]

    def updatedetailrecord(db, f):
        #     # put it exactly as sent in f, blank out any fields that are blank
        #     if 'id' not in f:
        #         return [False, 'id not in update dictionary']
        #     cur = db.cursor()
        #     id =  f['id']
        #     tag = "" if not 'tag' in f else f['tag']
        #     description = "" if not 'description' in f else f['description']
        #     delcode = "" if not 'delcode' in f else f['delcode']
        #     itinitials = "" if not 'itinitials' in f else f['itinitials']
        #     sql = "update invdelformdetails set tag = ?, description=?, delcode=?, itinitials=?, updated=getdate() where id = ?;"
        #     try:
        #         cur.execute(sql, (tag, description, delcode, itinitials, id))
        #         db.commit()
        #         cur.close # ?
        #         return True
        #     except Exception as e:
        #         return [e, sql]
        pass

    def _getdetailsfromtags(self, tags):
        if self.dbv:
            sql = "SELECT M.TagNumber as tag, D.description FROM RISDBase.dbo.tblFAAssetDetail D join RISDBase.dbo.tblFAAssetMaster M on M.ID = D.AssetMasterID where M.tagnumber in ({})".format(",".join("?"*len(tags)) )
            ans = self.dbv.execute_s(sql, tags)
            return ans

###########################
#the below is a manual test.
if __name__ == '__main__':

    username = "BYATES" # g.user.upper()

    today = ""
    itinitials = ""
    myinitials = username.upper()[:2]
    ittodaysdate = ""
    if 'itdepartment' in ['itdepartment']: # g.details['groups']:
        today = datetime.now().strftime("%x")
        itinitials = myinitials # prob unused, TODO fix
        ittodaysdate = today

    mdf = MyDeletionForms("byates", includeinactive=True)
    print(mdf.includeinactive)
    print(mdf)
    # for a in mdf.forms:
    #     print (a)

    t = DeletionForm(id=1)
    print(f"dba.spid: {t.dba.spid} dbv.spid: {t.dbv.spid}")
    t.fillinformdetails()
    print(t.lines[0])
    print(t.lines[1])

    print(t)
    t.notes = 'APs'
    t._updateheaderrecord()
    print(t)

    b = DeletionForm(id=1)
    print(b)

    #     rec['id'] = id # = submit.split(" ")[-1]
    #     rec['schooldeleting'] = request.form.get("sdel")
    #     rec['workorder'] =  request.form.get("wo")
    #     rec['notes'] = request.form.get("notes")
    #     rec['username'] = request.form.get("creator")
    #     ans = _updateheaderrecord(dbf, rec)
    #     if ans != True:
    #         flash("Error, that update failed")
    #     submit = "Edit {}".format(id)
    #     # return render_template('inventory/index.html',rows=['submit fixed?', submit, rec, ans])
    # elif re.match("^(Add New Detail line to Form)", submit):
    #     rec = {}
    #     rec['invid'] = id
    #     rec['tag'] = request.form.get("newtag")
    #     rec['description'] = request.form.get("newdescription")
    #     rec['delcode'] = request.form.get("newdelcode")
    #     rec['itinitials'] =  request.form.get("newitinitials")
    #     x = datetime.datetime.now()
    #     rec['dateitcleared'] = x.strftime("%x")
    #     ans = insertdetailrecord(dbf, rec)
    #     if ans != True:
    #         flash("Error inserting detail record {}".format(ans))
    #     submit = "Edit {}".format(id)
    # elif re.match("^(Update detail)", submit):
    #     detailid = submit.split(" ")[-1]
    #     rec = {}
    #     # updateformandcontinuewith, id = submit.rsplit(" ")
    #     rec['id'] = detailid # = submit.split(" ")[-1]
    #     rec['tag'] = request.form.get("tag{}".format(detailid))
    #     rec['description'] = request.form.get("description{}".format(detailid))
    #     rec['delcode'] = request.form.get("delcode{}".format(detailid))
    #     rec['itinitials'] =  request.form.get("itinitials{}".format(detailid))
    #     ans = updatedetailrecord(dbf, rec)
    #     if ans != True:
    #         flash("Error updating record {}".format(ans))
    #     submit = "Edit {}".format(id)
    #     # return render_template('inventory/index.html',rows=[{'submitN':submit, 'detailid':detailid,'id':id}])
    # elif re.match("^(Delete Detail)", submit):
    #     detailid = submit.split(" ")[-1]
    #     ans = _deletedetailrecord(dbf, detailid)
    #     if ans != True:
    #         flash("Error deleting record {}".format(ans))
    #     submit = "Edit {}".format(id)
    #     # return render_template('inventory/index.html',rows=[{'submitM':submit, 'detailid':detailid,'id':id}])
    # elif re.match("^(Flag inactive|Delete entire form)", submit):
    #     rec = {}
    #     rec['id'] = id
    #     if "Flag inactive" in submit:
    #         rec['active'] = 0
    #     elif "Delete entire form" in submit:
    #         rec['active'] = 2
    #     ans = _updateheaderrecord(dbf, rec)
    #     if ans != True:
    #         flash("Error, that update failed {}".format(ans))
    #     submit = ""
    #     id = "" # refresh to entry page
    #     # return render_template('inventory/index.html',rows=['259submit fixed?', submit, rec, ans])
    # elif re.match("^(Create & Start Scanning)", submit):
    #     # take top of form values and stick them in InvDelForms
    #     # then launch into adding scans with the ID
    #     notes = request.form.get("notes")
    #     sdel = request.form.get("sdel")
    #     wo = request.form.get("wo")
    #     creator = request.form.get("creator")
    #     cur = dbf.cursor() # had to get cursor to get .lastrowid
    #     id = cur.execute("insert into invdelform (schooldeleting, workorder, notes, username) VALUES (?,?,?,?)", (sdel, wo, notes, creator)).lastrowid
    #     dbf.commit()
    #     cur.close()
    #     submit = "Smartphone scan to {}".format(id)
    # elif re.match("^(Submit Tags)", submit) and not scanned:
    #     submit = "Edit {}".format(id)
    # elif re.match("^(Submit Tags)", submit) and scanned:
    #     added = []
    #     alreadyadded = []
    #     rows = []
    #     form = []
    #     if '\r\n' in scanned[0]:
    #         scanned = scanned[0].split('\r\n')
    #         if '' in scanned:
    #             scanned.remove('')
    #     # return render_template('inventory/index.html',rows=[{'286 submit=':submit, 'id':id, 'show':show, 'scanned':scanned}])

    #     details = _getdetailsfromtags(dbv, scanned)
    

    #     for d in details:
    #         if d['tag'] not in details:
    #             # add it to the form
    #             d['invid'] = id
    #             d['delcode'] = 'o' # default a better way?
    #             d['itinitials'] = itinitials # default a better way--kisok mode TODO
    #             # values = dict of all values for 1 row to insert into DB
    #             d['dateitcleared'] = today
    #             _insertdetailrecord(dbf, d)
    #             added.append(d)
    #         else:
    #             alreadyadded.append(d)
    #     if added:
    #         rows.append('These were added to Del form {}'.format(id))
    #         rows.append(added)
    #     if alreadyadded:
    #         rows.append('These were skipped, as they are already in this Del form {}'.format(id))
    #         rows.append(alreadyadded)
    #     if added:
    #         flash("{} new lines added to form {}".format(len(added), id))
    #     if alreadyadded:
    #         flash("{} Duplicate scans ignored".format(len(alreadyadded)))

    #     submit = "Edit {}".format(id)
    #     # return render_template('inventory/index.html',rows=rows, form=form )
    
    # # return render_template('inventory/index.html',rows=[{'287short circuit2 submit=':submit, 'id':id, 'show':show, 'scanned':scanned}])

    # # now send them to the correct output path
    # # new form since nothing good was selected
    # if not id and not submit:
    #     # show summaries for existing items, and allow add new delform
    #     form = []
    #     color = ""
    #     formtitle = ""
    #     sql = ""
    #     myform = ""
    #     if show == "inactive":
    #         sql = "select *,(select count(distinct tag) from invdelformdetails where invid=F.id) as tagcount from invdelform F where F.active = 0 order by updated desc;"
    #         color = "Pink"
    #         formtitle = "Inactive Deletion Forms"

    #     elif show == "deleted":
    #         sql = "select *,(select count(distinct tag) from invdelformdetails where invid=F.id) as tagcount from invdelform F where F.active = 2 order by updated desc;"
    #         color = "YellowGreen"
    #         formtitle = "Deleted Deletion Forms"

    #     else:
    #         sql = "select *,(select count(distinct tag) from invdelformdetails where invid=F.id) as tagcount from invdelform F where F.active = 1 order by updated desc;"
    #         color = "AliceBlue"
    #         formtitle = "Active Deletion Forms"
    #         myform = '<p style="text-align: right;"><a href="?show=inactive">View Inactive Forms</a></p>'
    #     rows = sqlfromdb(dbf, sql)
    #     # for r in/ rows:
    #     # return render_template('inventory/index.html',rows=[{'317 submit=':submit, 'rows':rows, 'sql':sql}])

    #     for f in rows:
    #         i = f['id']
    #         cnt = f['tagcount']
    #         s = "s" if cnt != 1 else ""
    #         myform += '<div style="border-radius: 10px; border:2px solid black; background: {}; padding:10px;"><h4>Deletion form number {} - contains {} tag{}</h4>'.format(color, i, cnt, s)

    #         v = "" if f['notes'] == None else f['notes']
    #         if v:
    #             myform += '\nPersonal notes: {}<br />'.format(v)

    #         v = "" if f['schooldeleting'] == None else f['schooldeleting']
    #         if v:
    #             myform += '\nSchool Unit Deleteing Items: {}<br />'.format(v)

    #         v = "" if f['workorder'] == None else f['workorder']
    #         if v:
    #             myform += '\nWork Order Num: {}<br />'.format(v)

    #         v = "" if f['username'] == None else f['username']
    #         if v:
    #             myform += '\nOwner username: {}<br />'.format(v)

    #         v = "" if f['updated'] == None else f['updated']
    #         if v:
    #             myform += '\nUpdated: {}<br />'.format(v)


    #         # myform += '<input type="submit" name="submit" value="View {}"> '.format(i)
    #         # myform += '\n<input type="submit" name="submit" value="Download {}"> '.format(i)
    #         myform += '<br />\n<input type="submit" name="submit" value="Edit {}"> '.format(i)
    #         myform += '\n<input type="submit" name="submit" value="Smartphone scan to {}"> '.format(i)
    #         myform += '\n<input type="submit" name="submit" value="Barcode scan or 10key to {}"> '.format(i)
    #         myform += '\n</div><hr />'
    #         form.append(myform)
    #         myform = ""
    #     if show:
    #         if not form:
    #             flash("no {} forms found".format(show))
    #             return redirect(url_for('.deletions'))
    #     else:
    #         form.append('<h3>Make a new Deletion form and start scanning</h3>')
    #         form.append('<label for="notes">Personal notes:</label><input type="text" id="notes" name="notes" ><br />')
    #         form.append('<label for="sdel">School Unit Deleteing Items:</label> <input type="text" id="sdel" name="sdel" value=""><br />')
    #         form.append('<label for="wo">Work Order Num:</label> <input type="text" id="wo" name="wo"><br />')
    #         form.append('<label for="creator">* Username of form owner:</label> <input type="text" id="creator" name="creator" value="{}" required><br />'.format(g.user))
    #         form.append('<input type="submit" name="submit" id="submit" value="Create & Start Scanning">')

    #     return render_template('inventory/index.html', form=form, formtitle=formtitle, pgtitle="Deletions")
    # elif re.match("^(Add)", submit):

    #     # return render_template('inventory/index.html',rows=[':158b', rec['id'],  r,  id, schooldeleting, workorder, notes, creator ] )
    #     return render_template('inventory/scan.html', pgtitle = 'Adding to deletion form {}'.format(id), id=id)
    # elif re.match("^(Download)", submit):
    #     headerinfo = _readheaderinfo(dbf, id)[0]
    #     fulldetails = _getdetails(dbf, id)
    #     tags =  sorted(list(set([t['tag'] for t in fulldetails])))
    #     wb = load_workbook(filename = '/var/www/flaskr/flaskr/static/FARetirementBLANK.xlsx')
    #     ws = wb.active

    #     # this header info will be copied to new sheets, so just do it once
    #     if 'schooldeleting' in headerinfo and headerinfo['schooldeleting']:
    #         ws['A13'].value = headerinfo['schooldeleting']
    #     if 'workorder' in headerinfo and headerinfo['workorder']:
    #         ws['I19'].value = headerinfo['workorder']
    #     ws['I13'] = datetime.today().strftime('%m / %d / %Y')
    #     # img = openpyxl.drawing.image.Image('roycesig.jpg')
    #     # img.anchor = 'A1'
    #     # ws.add_image(img)

    #     line = 25
    #     firsttag = ""
    #     lasttag = ""
    #     firstsheetname = "" # for forumla use on sheets>1
    #     # {'TagNumber':'57476','Description':'3Y TABLET REPAIR W/ADH $801-$900DOP'}
    #     for tag in tags:
    #         mylist = []
    #         mylist = [item for item in fulldetails if item["tag"] == tag]
    #         # return render_template('inventory/index.html',rows = ['blue', tag, mylist, fulldetails])
    #         dvals = []
    #         dvals = sorted(set([t['description'] for t in mylist]))

    #         if line == 25:
    #             firsttag = tag
    #         if (line > 45):
    #             ws.title = "{}-{}".format(firsttag, lasttag)
    #             if not firstsheetname:
    #                 firstsheetname = ws.title
    #             line = 25
    #             firsttag = tag
    #             ws2 = wb.copy_worksheet(ws)
    #             ws = ws2
    #             for t in range(25,46):
    #                 for r in (['B', 'C', 'I', 'K', 'L', 'M']):
    #                     ws['{}{}'.format(r, t)].value = ""
    #             ws['A13'] = "='{}'!A13".format(firstsheetname)
    #             ws['I13'] = "='{}'!I13".format(firstsheetname)
    #             ws['I19'] = "='{}'!I19".format(firstsheetname)

    #         t = ws['B{}'.format(line)]
    #         lasttag = tag
    #         t.value = tag
    #         t.alignment=Alignment(horizontal='center', vertical='center', shrink_to_fit=True)
    #         d = ws['C{}'.format(line)]
    #         # insert all the values here, if more than one, then make it DV, otherwise just one

    #         if dvals and len(dvals) > 1:
    #             d.value = dvals[0]
    #             dvallist = ','.join([re.sub('[,\'\"]', ' ', d) for d in dvals])
    #             # return render_template('inventory/index.html',rows = [tag, dvallist] )
    #             dv = DataValidation(type="list", formula1='"{}"'.format(dvallist), allow_blank=True, showErrorMessage = False)
    #             ws.add_data_validation(dv)
    #             dv.add(d)
    #             ws['I{}'.format(line)] = "."
    #         elif dvals:
    #             d.value = dvals[0]
    #         else:
    #             d.value = 'huh, no desc?'

    #         d.alignment=Alignment(horizontal='left', vertical='center', shrink_to_fit=True)
    #         c = ws['K{}'.format(line)]
    #         c.value = mylist[0]['delcode']
    #         c.alignment=Alignment(horizontal='center', vertical='center', shrink_to_fit=True)
    #         i = ws['L{}'.format(line)]
    #         i.value = mylist[0]['itinitials'] # 'BY'
    #         i.alignment=Alignment(horizontal='center', vertical='center', shrink_to_fit=True)
    #         d = ws['M{}'.format(line)]
    #         d.value = mylist[0]['dateitcleared'].strftime("%m-%d-%y")
    #         d.alignment=Alignment(horizontal='center', vertical='center', shrink_to_fit=True)
    #         line += 1
    #     ws.title = "{}-{}".format(firsttag, lasttag)

    #     # return render_template('inventory/index.html')
    #     # return render_template('inventory/index.html',rows = dvals)
        

    #     return send_file(
    #                     io.BytesIO(save_virtual_workbook(wb)),
    #                     attachment_filename='{}Delete{}.xlsx'.format(datetime.today().strftime('%Y%m%d'), id),
    #                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    #             )
    # elif re.match("^(Edit)", submit):
    #     form = []
    #     myform = ""
    #     mylist = []

    #     # put the header fields here with a submit of "Update header detail"
    #     ans = _readheaderinfo(dbf, id)
    #     if ans:
    #         headerinfo = ans[0]
    #     else:
    #         flash("failed to retrieve form {}, call Bryan, and/or quit hacking.".format(id))
    #         return redirect(url_for('.deletions'))
        
    #     form.append('<input type="hidden" name="delformid" value="{}"><br />'.format(id))
    #     form.append('<span style="text-align:right;"><input type="submit" name="submit" value="Delete entire form {}" style="color:red;" ><br />'.format(id))
    #     form.append('<input type="submit" name="submit" value="Flag inactive - form {}" style="color:yellow; background-color:darkgrey;" ><br /></span>'.format(id))

    #     # v = "" if not 'notes' in f else f['notes']
    #     # i = headerinfo['id']
    #     myform = '<div style="border-radius: 10px; border:2px solid black; background: #cce7ff; padding:10px;">Form ID: {}<br />'.format(id)

    #     # v = headerinfo['notes']
    #     # v = "" if v == None else v
    #     v = "" if not 'notes' in headerinfo else headerinfo['notes']
    #     return render_template('inventory/index.html',rows = ['498', v, 'blue', id, 'org', headerinfo, g.details['groups'] ])
    #     myform += '\n<label for="notes">Personal notes </label><input type="text" id="notes" name="notes" value="{}" ><br />'.format(v)

    #     # v = headerinfo['schooldeleting']
    #     # v = "" if v == None else v
    #     v = "" if not 'schooldeleting' in headerinfo else headerinfo['schooldeleting']
    #     myform += '\n<label for="sdel">School Unit Deleteing Items: </label><input type="text" id="sdel" name="sdel" value="{}"><br />'.format( v)

    #     # v = headerinfo['workorder']
    #     # v = "" if v == None else v
    #     v = "" if not 'workorder' in headerinfo else headerinfo['workorder']
    #     myform += '\n<label for="wo">Work Order Num: </label><input type="text" id="wo" name="wo" value="{}"><br />'.format(v)

    #     # v = headerinfo['username']
    #     # v = "" if v == None else v
    #     v = "" if not 'username' in headerinfo else headerinfo['username']
    #     myform += '\n<label for="creator">Owner username: </label><input type="text" id="creator" name="creator" value="{}" required><br />'.format(v)

    #     myform += '\n<input type="submit" name="submit" value="Update header {}"><br />'.format(id)
    #     myform += '\n</div>\n&nbsp;\n'
        
    #     form.append(myform)

    #     fulldetails = _getdetails(dbf, id)

    #     # return render_template('inventory/index.html',rows=[{'510 fulldetails':fulldetails,'v':v, 'id':id}])
    #     tags =  sorted(list(set([t['tag'] for t in fulldetails])))
        

    #     lentags = len(tags)
    #     # return render_template('inventory/index.html',rows = ['blue', id, tags, fulldetails])

    #     for tag in tags:
    #         mylist = []
    #         # form.append("I was here once, with tag{}".format(tag))
    #         myform = '<div style="border-radius: 10px; border:2px solid black; background: AliceBlue; padding:10px;">'

    #         # gather tag description(s) into mylist and work with each of those as clump
    #         mylist = [item for item in fulldetails if item["tag"] == tag]
    #         # return render_template('inventory/index.html',rows = ['blue', tag, mylist, fulldetails])
    #         formdisabled  = ""
    #         for row in mylist:
    #             if myform[-7] != '10px;">':
    #                 myform += "<br />"
    #             myform += 'Row {}\n<input type="text" value="{}" name="tag{}" size="6">'.format(row['id'], row['tag'], row['id'])
    #             myform += '\n<input type="text" value="{}" name="description{}" >'.format(row['description'], row['id'])
    #             myform += '\n<input type="text" value="{}" name="delcode{}" size="3">'.format(row['delcode'], row['id'])
    #             myform += '\n<input type="text" value="{}" name="itinitials{}" size="3" {}>'.format(row['itinitials'], row['id'], formdisabled)
    #             myform += '\n<input type="text" value="{}" name="dateitcleared{}" size="15">'.format(row['dateitcleared'], row['id'])
    #             myform += '\n<input type="submit" name="submit" value="Update detail {}">'.format(row['id'])
    #             myform += '\n<input type="submit" name="submit" style="color:red;" value="Delete Detail {}">'.format(row['id'])
    #         form.append(myform)
    #         form.append('\n</div>\n&nbsp;\n')

    #     form.append('<br /><div style="border-radius: 10px; border:2px solid black; padding:10px;">')
    #     form.append('Add a New Row <input type="text" value="" name="newtag" size="6">')
    #     form.append('<input type="text" value="" name="newdescription">')
    #     form.append('<input type="text" value="O" name="newdelcode" size="3">')
    #     form.append('<input type="text" value="{}" name="newitinitials" size="3">'.format(itinitials))
    #     form.append('<input type="text" value="{}" name="newdateitcleared" size="15">'.format(datetime.today().strftime('%m-%d-%Y')))
    #     form.append('<input type="submit" name="submit" value="Add New Detail line to Form {}">\n</div><br />\n'.format(id))
       
    #     form.append('\n<input type="submit" name="submit" value="Smartphone scan to {}"> '.format(id))
    #     form.append('\n<input type="submit" name="submit" value="Barcode scan or 10key to {}"> '.format(id))
    #     form.append('<input type="submit" name="submit" value="Download {}">'.format(id))

    #     return render_template('inventory/index.html', form=form, formtitle = "Edit Deletion Form {} ({} tags)".format(id, lentags))
    # elif re.match("^(Cancel)", submit):
    #     return redirect(url_for('.deletions'))
    # elif re.match("^(Smartphone scan to)", submit):
    #     return render_template('inventory/scan.html', pgtitle = 'scan deletions', id=id)
    # elif re.match("^(Barcode scan or 10key to)", submit):
    #     myform = '<textarea name="scanned" rows="10" cols="20"></textarea>'
    #     myform += '<input type="hidden" name="id" value="{}"><br />'.format(id)
    #     myform += '<input type="submit" name="submit" value="Submit Tags"></<br />'
    #     return render_template('inventory/index.html', pgtitle = 'barcode scan deletions', form=[myform])
        
    # return render_template('inventory/index.html',rows=['551catchall', id, {'submit':submit}])


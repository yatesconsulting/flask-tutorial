import sys
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial') # required for "from flaskr import"
from flaskr import pyodbc_db
import os
import re
import filedate
from datetime import datetime

class ExportNewAttachments():
    '''getmesomesqlfiles() grabs all new attachments from sected DB since noted ID in text file,
     and turndbrowsintofiles() downloads them locally and updates that ID file'''
    def __init__(self, db='TmsEPly'):
        self.db = db
        self.spath = f'C:/Users/bryany/Desktop/GitHub/flask-tutorial/Attachments/{self.db}'
        self.idnumfile = f'{self.spath}/donethroughATTACHMENT_ID.txt' # one in each DB folder
        self.errorlog = f'{self.spath}/errors.txt'
        self.donethroughnum = self.donethroughATTACHMENT_ID()
        self.pattern = re.compile("43\d\d\d\d\d")
        # these could probably just go here, but it's slighly more flexible if not
        # rows = self.getmesomesqlfiles()
        # self.turndbrowsintofiles(rows)

    def turndbrowsintofiles(self, rows):
        if rows:
            for i in rows:
                try:
                    # print(repr(i[1]))
                    fpath = "{}/{}".format(self.spath, i[0])
                    #  create directory if missing
                    if not os.path.isdir(fpath):
                        os.mkdir(fpath)
                    id_num = self.findmeanidin(i[2])
                    filename = i[1].replace("~MAGIC~", "~{}~".format(id_num))
                    
                    fpathfilename = "{}/{}".format(fpath,filename)
                    if not os.path.exists(fpathfilename):
                        print(f"Creating id:{i[6]}, name: {fpathfilename}")
                        open(fpathfilename, 'wb').write(i[3])
                        file_size = os.path.getsize(fpathfilename)
                        if file_size != i[4]:
                            err = f"SIZE DIFFERS File Size is {file_size} bytes, compared to db size of {i[4]} for ATTACHMENT_ID={i[6]}, filename={fpathfilename}\n"
                            print(err)
                            open(self.errorlog, 'a').write(f"{datetime.now()} {err}")

                        filedate.File(fpathfilename).set(
                            created = i[5],
                            modified = i[5]
                        )
                    else:
                        # should not see these after testing
                        print("Skipping existing file {}".format(fpathfilename))
                        # pass
                except:
                    pass
            self.updatedonethroughATTACHMENT_ID(i[6])

    def donethroughATTACHMENT_ID(self):
        '''return -100 or the number in the text file telling me where to look +1 and up from'''
        if not os.path.exists(self.idnumfile):
            return -100

        with open(self.idnumfile, 'r') as f:
            num = f.read().rstrip()
        return num

    def updatedonethroughATTACHMENT_ID(self, newlatestidnum):
        open(self.idnumfile, 'w').write(f"{newlatestidnum}")
        # more pythonic, but less cool
        # with open(idfile, 'w') as f:
        #     f.write(newlatestidnum)

    def findmeanidin(self, fn):
        ans = self.pattern.search(fn) # global variable pattern slightly sloppy
        if ans:
            return ans[0]
        else:
            return ""

    def getmesomesqlfiles(self):
        sql = """Select -- top 100
        replace(isnull(ATTACHMENT_TYPE_CDE,'NULL'),' ','') as dir -- 0
        ,replace(
            isnull(ATTACHMENT_TYPE_CDE,'')
            + '~MAGIC~' + 
            isnull(ATTACHMENT_NAME,'')
            + '~' + 
        isnull(replace(CONVERT(varchar(16), CREATION_DTE, 126),':',''),'') 
            + '~' +
        isnull(replace(CONVERT(varchar(16), JOB_TIME, 126),':',''),'') 
            + '.' + isnull(FORMAT_CDE,'Txt') -- fail safe, and look strange		 
            ,' ','') as fname -- 1
            , isnull(ATTACHMENT_NAME,'') aname -- 2
            , attachment -- 3
            , datalength(Attachment) as alen -- 4
            ,case when isnull(CREATION_DTE,'1900-01-01 00:00:00.000')='1900-01-01 00:00:00.000' 
            then JOB_TIME else CREATION_DTE end as creationdate -- 5
            , ATTACHMENT_ID as id -- 6
            FROM {}..Attachment --where ATTACHMENT_TYPE_CDE like '%bu%'
            where ATTACHMENT_ID > ?
            order by ATTACHMENT_ID;""".format(self.db)
            #  FROM {}..Attachment  where attachment_id={} order by 1;""".format(self.db,92509)
            # print(sql)
        db = pyodbc_db.MSSQL_DB_Conn()
        # co   nn = pymssql.connect(server="<server IP/URL>",user="<username>",password="<password>",database="<datebase_name>")
        cursor = db.cursor
        cursor.execute(sql, [self.donethroughnum])
        ans = cursor.fetchall()
        cursor.close()
        print("Found {} attachments in {} above the number {}".format(len(ans), self.db, self.donethroughnum))
        return ans

if __name__ == '__main__':

    db = "" # just put tmseprd in here if you want to default it to live, and run it from VScode

    # use the passed variable to the program, if available, and a good choice
    if sys.argv and len(sys.argv) > 1 and sys.argv[1] and sys.argv[1].lower() in ['tmseply','tmseprd']:
        db = sys.argv[1]
    else:
        db = 'TmsEPly' # not case sensitive
    print(f"{datetime.now()} Starting program using {db}, please be patient, this takes a while.")
    
    t = ExportNewAttachments(db)
    print(f"Looking for IDs > {t.donethroughnum} per {t.idnumfile}")
    # if program is taking forever to run, and returning -100 for the #, check your file paths
    rows = t.getmesomesqlfiles()
    t.turndbrowsintofiles(rows)
    print(f"{datetime.now()} Finished.")

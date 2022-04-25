# import pymssql
# import imp
import sys
import os
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial/flaskr') # required for from flaskr.models.... import
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial') # required for ?import?
# from flaskr import pyodbc_db
from flaskr import pyodbc_db
from myflaskrsecrets import dbname ## mcn_connect for us
import re

# select min(id_num),max(id_num) from NameMaster where name_type = 'p' -- 4300028	4367880
pattern = re.compile("43\d\d\d\d\d")

def findmeanidin(fn):
    ans = pattern.search(fn) # global variable pattern slightly sloppy
    if ans:
        return ans[0]
    else:
        return ""

sql = """Select
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
    ,' ','') as ReportFileName -- 1
    , isnull(ATTACHMENT_NAME,'') -- 2
    , Attachment -- 3
    , datalength(Attachment) -- 4 TODO
    FROM {}..Attachment --where ATTACHMENT_TYPE_CDE like '%bu%'
    order by 2;""".format('TmsEPly')
        #  FROM {}..Attachment  where attachment_id={} order by 1;""".format('TmsEPly',92509)
# print(sql)
db = pyodbc_db.MSSQL_DB_Conn()
# conn = pymssql.connect(server="<server IP/URL>",user="<username>",password="<password>",database="<datebase_name>")
cursor = db.cursor
cursor.execute(sql)
row = cursor.fetchall()
for i in row:
    try:
        # print(repr(i[1]))
        fpath = "Attachments\\{}".format(i[0])
        #  create directory if missing
        if not os.path.isdir(fpath):
            os.mkdir(fpath)
        id_num = findmeanidin(i[2])
        filename = i[1].replace("~MAGIC~", "~{}~".format(id_num))
        
        fpathfilename = "{}\\{}".format(fpath,filename)
        ## make this non-destructive, only create files that need creating
        print("Creating {}, if it needs it...".format(fpathfilename))
        # select datalength(field) from table where PK = Expression
        with open("{}\\{}".format(fpath, filename), 'wb') as outfile:
            outfile.write(i[3])
            outfile.close()
            print("Filename Saved as: {}\\{}".format(fpath, filename))
            # check file size to match with [4]
            # now update it's creation time so it can be sorted by that in Windows?
            # https://stackoverflow.com/questions/4996405/how-do-i-change-the-file-creation-date-of-a-windows-file
    except:
        pass
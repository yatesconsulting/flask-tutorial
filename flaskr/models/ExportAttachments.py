# import pymssql
import sys
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial/flaskr') # required for from flaskr.models.... import
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial') # required for ?import?
# from flaskr import pyodbc_db
from flaskr import pyodbc_db
from myflaskrsecrets import dbname ## mcn_connect for us

sql = """Select replace(isnull(ATTACHMENT_NAME,'')
		 + '__' + isnull(ATTACHMENT_TYPE_CDE,'')
		 + '__' + isnull(replace(CONVERT(varchar(16), CREATION_DTE, 126),':',''),'') 
         + '__id' + cast(attachment_id as varchar(20)) 
		 + '.' + isnull(FORMAT_CDE,'Txt') -- fail safe, and look strange		 
		 ,' ','') as ReportFileName
         , Attachment
         FROM {}..Attachment order by 1;""".format('TmsEPly')
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
        with open("Attachments\\{}".format(i[0]), 'wb') as outfile:
            outfile.write(i[1])
            outfile.close()
            print("Filename Saved as: " + i[0])
            # now update it's creation time so it can be sorted by that in Windows?
            # https://stackoverflow.com/questions/4996405/how-do-i-change-the-file-creation-date-of-a-windows-file
    except:
        pass
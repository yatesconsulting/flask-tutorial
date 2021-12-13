#!/usr/bin/python

import sys
import cgi
# import cgitb # remove when live
# cgitb.enable()
# cgitb.enable(display=0, logdir="/users/byates/downloads/cgilogs")
import os # used for environment variables, probably differs in Linux
# import pymysql.cursors
# import pymysql
import risd # dbconnect info

pgtitle = "Inventory"
# techs = []
form = cgi.FieldStorage()
sqlwhere = []

def head(pgtitle):
    print ("Content-type:text/html\n\n")
    # <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/> # maybe move all this into the header???
    # <!DOCTYPE html>
    print ('<html>\n<head>\n<meta charset="utf-8" />')
    print('<title>{}</title>\n'.format(pgtitle))
    print("""
<style>
    @media print {
        .dataTables_wrapper  {
            page-break-after: always;
        }
        .dataTables_wrapper:last-child {
            page-break-after: auto;
        }
        .noprint {
            display: none !important;
        }
    }
</style>
</head>
<body>
<div class="noprint">""")
    print("<h1>{}</h1>\n".format(pgtitle))

def fromsql(mydb, sql):
    cursor = mydb.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    return results

############ Start logic here  ##################
head(pgtitle)
# print(sys.path) 
# print("ver: {}".format(os.system("python -V")))

for k, v in os.environ.items(): print("ENV: {}={}<br />".format(k,v))
# cgi.print_form(form)
print("test by {}<br/></body></html>".format(os.getenv("AUTH_USER")))
exit()

# UNMAPPED_REMOTE_USER	RISD\BYates
# REMOTE_USER	RISD\BYates
# AUTH_USER	RISD\BYates
# LOGON_USER	RISD\BYates

# ensure this page is internal use only
if os.getenv("AUTH_USER"):
    techusername = os.getenv("AUTH_USER").replace('RISD\\','')
else:
    # report misuse of auth program without auth user TODO, and exit
    print ("Unauthorized</body></html>")
    exit()

# enable SQL
mydb = risd.getdbconnection('sftp')
allous = fromsql(mydb, "SELECT distinct orgUnitPath FROM CHROMEBOOKS order by orgUnitPath")
# allschools = []
# for a in allous:
#     parts = allous.split('/')


print('<table class="noprint" width="100%">\n <tr>\n  <td width="50%">')
# see if the page has sumbitted data
filtertype = ""
vals = []
# sanitize selection query
# ou=0,6,7 or s=BES,MHE formats only

if form.getfirst("s"):
    # print("Form type is School, not OU".format(form.getlist("s")))
    filtertype = "s"
    vals = form.getlist("s")
elif form.getfirst("ou"):
    # print("Form type is ou?{}".format(form.getlist("ou")))
    filtertype = "ou"
    vals = form.getlist("ou")

# show filters by school, with s=<vals> selected
if filtertype == "s":
    cleans = showschoolfilters(vals)
    t = []
    for a in cleans:
        t.append("orgUnitPath like CONCAT('%', '{}', '%')".format(a))
        # orgUnitPath like CONCAT('%', T.schoolinitials, '%') 
    sqlwhere.append("(({}))".format(") or (".join(t)))
else:
    showschoolfilters()
print("</td><td>")

# show filters by OU, with ou=<vals> selected
if filtertype == "ou":
    cleans = showoufilters(allous, vals)
    sqlwhere.append("orgUnitPath in ('{}')".format("','".join(cleans)))
else:
    showoufilters(allous)
    # show selected results
print("</td></tr></table><hr />")
print('<p align="right">Global search:' )
print('<input type="text" class="global_filter" id="global_filter"></p></div>')

if filtertype:
    showresults(sqlwhere = sqlwhere)

# resultstech = fromsql(mydb, "SELECT * FROM techs")
# print("<h2>Techs:</h2>")
# for x in resultstech:
#     techs.append(x["techusername"])
# techs = sorted(set(techs))
# for t in techs:
#     print("{}<br />".format(t))
#    # if ("\\{};;;".format(t) in "{};;;".format(os.getenv("AUTH_USER"))):
#    #     print("The one above this is you!")

# print("<table><tr><td>only showing for tech {}</td></tr>".format(techusername))

print ("<p class=\"noprint\" style=\"text-align: right;\"><i>User {} logged in</i></p>".format(os.getenv("AUTH_USER")))
print ("</body>\n<html>")
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

pgtitle = "Active Chromebooks"
# techs = []
form = cgi.FieldStorage()
sqlwhere = []

def head(pgtitle):
    print ("Content-type:text/html\n\n")
    print ("<html>\n<head>\n<title>{}</title>\n".format(pgtitle))
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

<link rel="stylesheet" type="text/css" href="/py/jquery.dataTables.css">
<script src="/py/jquery-3.1.1.min.js"></script>
<script type="text/javascript" charset="utf8" src="/py/jquery.dataTables.js"></script>
<script type="text/javascript" class="init">
function filterGlobal() {
  $('table.display').DataTable().search(
    $('#global_filter').val()
  ).draw();
}
 
$(document).ready( function () {
    $('table.display').DataTable( {
    "paging": false,
    "searching": true,
    "ordering": true,
    "processing": false,
    "infoEmpty": false,
//    "order": [0], // seems to be the default
    "dom": 'lrtp' // removes search boxes, and summary 
    });

// https://datatables.net/reference/option/dom
// dom: 'lrtip'
//    l - length changing input control
//    f - filtering input
//    t - The table!
//    i - Table information summary
//    p - pagination control
///    r - processing display element

    $('input.global_filter').on('keyup click', function () {
    filterGlobal();
    });

} ); 
</script>
</head>
<body>
<div class="noprint">""")
    print("<h1>{}</h1>\n".format(pgtitle))

def fromsql(mydb, sql):
    cursor = mydb.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    return results

def isvalidschool(school):
    school = school.upper()
    if ",{},".format(school) in ",BES,BMS,MVM,DNE,MMS,EGP,ECE,NLE,MHE,MAE,MON,SUN,SMS,PES,PEC,GHS,UHS,ECHS,RHS,VVE,WAE,SGMS,":
        return True
    return False

def querystringcleaner(qs):
    res = []
    if 'ou=' in qs:
        for a in qs[3:].split(','):
            if a.isdigit():
                res.append(a)
        return ('ou',res)
    elif 's=' in qs:
        for a in qs[2:].split(','):
            if isvalidschool(a):
                a = a.upper()
                if a == "PES":
                    res.append('PEC')
                else:
                    res.append(a)
        return('s',res)
    return (False,False)

def showschoolfilters(schools=[]):
    print('<form id="school">\nChoose one or more OUs<br />')
    print('<select name="s" form="school" size="10" multiple>')
    s = ""
    ans = []
    for a in "BES,BMS,MVM,DNE,MMS,EGP,ECE,NLE,MHE,MAE,MON,SGMS,SUN,SMS,PEC,GHS,UHS,ECHS,RHS,VVE,WAE".split(','):
        if a in schools:
            s = " selected"
            ans.append(a)
        else:
            s = ""
        print("<option value=\"{}\"{}>{}</option>".format(a, s, a))
    print('</select><br /><br /><input type="submit"></form>')
    return ans

def showoufilters(allous, ous=[]):
    print('<form id="ou">\nChoose one or more Schools<br />')
    print('<select name="ou" form="ou" size = "20" multiple = "multiple">')
    o = ""
    ans = []
    for a in range(len(allous)):
        if "{}".format(a) in ous:
            s = " selected"
            ans.append(allous[a]['orgUnitPath'])
        else:
            s = ""
        print("<option value=\"{}\"{}>{}</option>".format(a, s, allous[a]['orgUnitPath']))
    print('</select><br /><br /><input type="submit"></form>')
    return ans

def showresults(cols = ['orgUnitPath','annotatedAssetId as Tag','serialNumber as Serial'], sqlwhere = []):
    sql = "select {} from CHROMEBOOKS where status = 'ACTIVE' ".format(', '.join(cols))
    # there should be more, from the where clause, but let's check before assuming
    if sqlwhere:
        sql += "and {} ".format(" and ".join(sqlwhere))
    sql += "order by 1"
    # print("SQL={}".format(sql))
    cursorcb = mydb.cursor()
    cursorcb.execute(sql)
    myresult = cursorcb.fetchall()

    # print("cursor description: {}<br />".format(cursorcb.description ))

    num_fields = len(cursorcb.description)
    field_names = [i[0] for i in cursorcb.description]

    line = 0
    lastou = ""

    # figure out the header row, as it could be used twice, <th>'s
    hrow = []
    for h in field_names:
        if h == 'orgUnitPath':
            continue
        hrow.append("<th>{}</th>".format(h))

    for x in myresult:
        # print("<!-- x={} -->".format(x))
        line += 1
        if lastou != x["orgUnitPath"]:
            if line > 1:
                print('</tbody></table><br class="noprint">')
            print('<table id="" class="display" border="1" cellpadding="5" width="auto">\n <thead>\n')
            parts = x["orgUnitPath"].split('/')
            prettyname = "{} - {}".format(parts[-2],parts[-1])
            print('<tr><th colspan="{}">{}</th></tr>'.format(num_fields-1, prettyname))

            # header row, choose fields if desired, but keep orgunitpath TODO
            print("<tr>{}</tr></thead>\n\n".format('\n'.join(hrow)))
            # print("<tfoot><tr>{}</tr></tfoot>\n\n".format(hrow))
            print('<tbody>')
            lastou = x["orgUnitPath"]
        
        print (" <tr>")
        for h in field_names:
            if 'orgUnitPath' == h:
                continue
            if x[h]:
                print("  <td>{}</td>".format(x[h]))
            else:
                print("  <td>\&nbsp;</td>")
        print(" </tr>")
    print("</tbody></table>")


############ Start logic here  ##################
head(pgtitle)
# print(sys.path) 
# print("ver: {}".format(os.system("python -V")))

# for k, v in os.environ.items(): print("ENV: {}={}<br />".format(k,v))
# cgi.print_form(form)
# print("test by {}<br/></body></html>".format(os.getenv("AUTH_USER")))
# exit()

# UNMAPPED_REMOTE_USER	RISD\BYates
# REMOTE_USER	RISD\BYates
# AUTH_USER	RISD\BYates
# LOGON_USER	RISD\BYates

# ensure this page is internal use only
if os.getenv("AUTH_USER"):
    techusername = os.getenv("AUTH_USER").replace('RISD\\','')
else:
    # techusername = "jsoltero" # command line use testing, remove in live
    # report misuse of auth program or improper installation; no auth user TODO
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

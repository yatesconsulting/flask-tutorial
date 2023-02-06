# copy this file to myflaskrsecrets.py and then edit it, don't forget the R in flasrkR

# mssql databse secrets
dbserver = "localhost" # ip or name of database server
dbname = "web" # rename to databasename
dbuid = "user" # use Trusted_Connection if desired
dbpwd = "password"

jdbname = "TmsEPly"
tbldid = '{}..BAY_DupIDs'.format(dbname)
tbldip = '{}..BAY_DupsInProgress'.format(dbname)
tblxkeys = '{}..BAY_DupExtraKeys'.format(dbname)
tblmerges = '{}..BAY_dipidMergeSelections'.format(dbname)
tblnamemaster = '{}..NameMaster'.format(jdbname)
tblPKConstraints = '{}..BAY_PKConstraints'.format(dbname)
tblsysProcedures = '{}.sys.procedures'.format(dbname)

ldapserver = "localhost"
ldapdomain = "@somwhere.over.the.ra" # suffix, including the @
# ldapuser = 'serviceusername' # probably needed for group membership TODO
# ldappwd = 'Real!~lyGo0000000dPas5ss55s44ss321w()rd!'

# app secrets
secret_key = "ssupersecretet*pleaseimprove"

import sys
# import os
# from tabnanny import verbose
# from this import d
# sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial/flaskr') # required for from flaskr.models.... import
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial') # required for ?import?
from flaskr import pyodbc_db
# import re
# from os.path import exists as file_exists
# import filedate
# import glob
from datetime import datetime
# import argparse

class SSNTransposedCheck():
    '''checks for 2 numbers transposed in SSNs'''
    def __init__(self, db='TmsEPly'):
        self.db = db
        self.results = []
        self.checkfortransposedssns(self.getmesomessns())

    def getmesomessns(self):
        '''grab all 9 digit ssns from db'''
        sql = f"""select -- top 10 
            cast(ssn as char(9)) as ssn
            , b.id_num
            , n.last_name
            , n.first_name
            , n.birth_name
            , n.preferred_name
            from {self.db}..BIOGRAPH_MASTER B
            join {self.db}..NameMaster N
              on N.id_num = B.id_num
            where ssn between 
            100000000 and 999999999"""
        db = pyodbc_db.MSSQL_DB_Conn()
        cursor = db.cursor
        cursor.execute(sql) # fiix to use execute_s and clean this mess up
        ans = cursor.fetchall()
        cursor.close()
        # print("Found {} ssns to review from {}".format(len(ans), self.db))
        return ans
    
    def checkfortransposedssns(self, dbrows):
        '''transposed digits are like 12 where it should be 21, all other 7 digits are the same'''

        ssntoid = {}
        for a in dbrows:
            ssntoid[a[0]] = {'id_num':a[1], 'last_name':a[2], 'first_name':a[3], 
                'birth_name':a[4], 'preferred_name':a[5]}
        ssns = ssntoid.keys()
        ans = []
        for s in ssns:
            # s = thisssnrow[0]
            mytry = []
            for ab in range(8):
                if s[ab] != s[ab + 1]:
                    mytry.append(f"{s[:ab]}{s[ab+1]}{s[ab]}{s[ab+2:]}")
            for a in mytry:
                if a in ssns:
                    # print(f"Found a possibility here, {s} and {a}")
                    if int(a) < int(s):
                        v = f"{a}-{s}"
                    elif int(a) == int(s):
                        v = ""
                    else:
                        v = f"{s}-{a}"
                    if v and v not in ans:
                        ans.append(v)
        if ans:
            self.results.append("ssn       id_num  last, first   birth_name    preferred_name")
            # print("ssn       id_num  last, first   birth_name    preferred_name")
            lesslikely = []
            for a in ans:
                l,r = a.split('-')
                ll = ssntoid[l]
                rr = ssntoid[r]
                lln = ll['last_name']
                lfn = ll['first_name']
                rln = rr['last_name']
                rfn = rr['first_name']
                llin = f"{l} {ll['id_num']} {lln}, {lfn}  {ll['birth_name']}  {ll['preferred_name']}"
                rlin = f"{r} {rr['id_num']} {rln}, {rfn}  {rr['birth_name']}  {rr['preferred_name']}"
                tl = f"{llin}\n{rlin}\n"
                if lfn[:4].lower() == rfn[:4].lower() or lln[:4].lower() == rln[:4].lower():
                    # print(tl)
                    self.results.append(llin)
                    self.results.append(rlin)
                else:
                    lesslikely.append(llin)
                    lesslikely.append(rlin)

            if lesslikely:
                # print("These are a bit less likely:")
                self.results.append("These are a bit less likely.")
                for a in lesslikely:
                    # print(a)
                    self.results.append(a)

if __name__ == '__main__':

    db = "" # just put tmseprd in here if you want to default it to live, and run it from VScode

    # use the passed variable to the program, if available, and a good choice
    if sys.argv and len(sys.argv) > 1 and sys.argv[1] and sys.argv[1].lower() in ['tmseply','tmseprd']:
        db = sys.argv[1]
    else:
        db = 'TmsEPly' # not case sensitive
    print(f"started at {datetime.now()}")
    t = SSNTransposedCheck(db)
    print(t.results)
    print("try2")
    print(t.results[-1])
    # print(f"and the less likely  ones: {t.lesslikely}")
    print(f"finished at {datetime.now()}")


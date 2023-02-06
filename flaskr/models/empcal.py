#!/var/www/flaskr/venv/bin/python
#  this might only be needed when running from the command line, testing
import sys
sys.path.insert(0, r'/var/www/flaskr')
sys.path.insert(0, r'/var/www/flaskr/flaskr')
from flaskr import pyodbc_db_assessment as dbsource
from myflaskrsecrets import dbserverassessment, dbnameassessment, dbuidassessment, dbpwdassessment

import datetime

class EmpCal():
    def __init__(self, calnum, schoolyear="" ):
        # calnum internal ID from db, int
        self._blankday = { 'dom': 0, 'holiday':'', 'style':'', 'wcount': 0 }
        # self.dbname = dbserverassessment
        self.dba = dbsource.MSSQL_DB_Conn_assessment() # db=self.dbname)
        self.title = "" # String: ELEMENTARY PRINCIPALS, All Employees, or multiple titles, for example
        self.workdays = [] # 242, for example, the number of workdays in this calendar
        self.calnum = [] # database id(s) of calendars to use
        self.schoolyear = "" # = schoolyear or self._schoolyeardefault() # 2020-2021, for example, maybe not important
        if (self._sanitizecalnum(calnum)): # fills self.title, calnum, schoolyear & workdays
            # self.cal = {}
            self.raw = []
            self._fillcaldetails() # fills self.raw & workdays
            self.calmonthnames = []
            self.calweeks = []
            self._fillcalweeklydetails() # depends on self.raw, fills self.calweeks & calmonthnames

    def __str__ (self):
        return f"EmpCal({self.calnum})"

    def _sanitizecalnum(self, calnum):
        cksql = True
        if type(calnum) is not list:
            if calnum == "all":
                sqlr = self.dba.execute_s("select distinct id,workdays,schoolyear from EmpCalendars where status = 'active' and includeincsvexport = 1")
                self.calnum = [i["id"] for i in sqlr]
                self.title = "All Employee Calendars"
                self.workdays = [i["workdays"] for i in sqlr]
                cksql = False
            else:
                calnum = [ calnum ]
        if cksql:
            '''check each of the calnums in the list to verify they are valid in the DB'''
            for cn in calnum:
                a = []
                a = self.dba.execute_s(f"select distinct id,description,workdays,schoolyear from EmpCalendars where id={cn}")
                if len(a) == 0:
                    calnum.remove(cn)
                else:
                    self.title += f"{a[0]['description']}, "
                    self.calnum.append(a[0]["id"])
                    self.workdays.append(a[0]["workdays"])
            self.title = self.title[:-2]
        
        if len(self.calnum) > 0:
            a = self.dba.execute_s("select distinct schoolyear from EmpCalendars where id in ({})".format(",".join([str(a) for a in self.calnum])))
            if len(a) > 1:
                ''' odd, I guess I'll show them all, but this might be a bad decision'''
                self.schoolyear = ", ".join([b["schoolyear"] for b in a])
            else:
                self.schoolyear = a[0]["schoolyear"]
            return True
        else:
            self.title = 'No Calendar Selected'
            return False


    def _closeoutmonthsifneeded(self):
        for i in range(12):
            while len(self.calweeks[i]) < 5:
                self.calweeks[i].append([])
            for ii in range(5):
                while len(self.calweeks[i][ii]) < 5:
                    self.calweeks[i][ii].append(self._blankday)

    def _fillcaldetails(self):
        '''fills self.raw with calendar(s) detail from SQL'''
        ans = []
        for mycalnum in self.calnum:
            sql =   f"select * from fn_EmployeeCalendarDates({mycalnum})"
            ans.extend(self.dba.execute_s(sql))
        self.raw = ans

    def _fillcalweeklydetails(self):
        calmonindex = -1 # so += 1 will start at 0
        weeknum = 0
        lastweeknum = -1 # so they're different to start
        month = lastmonth = ""
        howmanyblanksIneed = {'Tuesday':1, 'Wednesday':2, 'Thursday':3, 'Friday':4}

        for a in self.raw:
            # if not self.title and a['CalName']:
            #     self.title = a['CalName']
            # if not self.workdays and a['WorkDays']:
            #     self.workdays = a['WorkDays']

            month = a['Month']
            dow = a['DayofWeek']
            holiday = a['Holiday']
            wcount = a['WorkdayCount']
            dom = a['dom']

            if dow in ['Saturday', 'Sunday']:
                pass

            elif lastmonth != month:
                self.calmonthnames.append(month)
                calmonindex += 1
                weeknum = 0
                self.calweeks.append([])
                self.calweeks[calmonindex].append([])
                if dow in howmanyblanksIneed:
                    n = howmanyblanksIneed[dow]
                    for temp in range(n):
                        self.calweeks[calmonindex][weeknum].append(self._blankday)
                lastmonth = month

            if dow not in ['Saturday', 'Sunday']:
                if holiday:
                    if wcount:
                        style = 'red'
                    else:
                        style = 'grn'
                else:
                    style = ''
                if len(self.calweeks[calmonindex][weeknum]) == 5:
                    self.calweeks[calmonindex].append([])
                    weeknum += 1
                self.calweeks[calmonindex][weeknum].append({'dom': dom, 'holiday':holiday, 'style':style, 'wcount': wcount })

        self._closeoutmonthsifneeded() # pad out all cals to 5x5

    def _schoolyeardefault(self):
        ''' return '2022-2023' by extracting it from database name, RISDPlayFY202122, for ex'''
        # if the current month is 
        # yrp1last2 = self.dbvyr[-4:-2]
        # yrp2last2 = self.dbvyr[-2:]
        # not y21k compliant notTODO #donotcare
        return '2022-2023'
        return ("20{}-20{}".format(yrp1last2, yrp2last2))

    def rawholidays(self):
        ''' sort throught self.raw and return self.raw for holidays only '''
        h = []
        for a in self.raw:
            if a['Holiday']:
                h.append(a)
        return h


    def _pad(self, num, padby):
        pads = "0" * padby
        return f"{pads}{num}"[-padby:]

    def datesholidsaywithfor(self):
        cleanerevents = []
        events = self.rawholidays()
        doyevents = set([f'"{self._pad(dict["DOY"],3)}-{dict["Holiday"]}"' for dict in events ])

        # sorted by doy, consolidate holidays with all calnames using it down to "all employees"
        # works pretty good, 4th of july and labor day could probably be "all" also, but some end contract dates are after/before the holiday
        for d in sorted(doyevents):
            workingset = [dict for dict in events if (f'"{self._pad(dict["DOY"],3)}-{dict["Holiday"]}"' == d)]
            if len(self.calnum) > 1 and len(workingset) == len(self.calnum):
                cleanerevents.append({'Date': f'{workingset[0]["Date"]}', 'Holidaywithfor': f'{workingset[0]["Holiday"]} for all employees'})
            else:
                for z in workingset:
                    cleanerevents.append({"Date": z["Date"], "Holidaywithfor":f'{z["Holiday"]} for {z["CalName"]}'})
        return cleanerevents


###########################
#the below is a manual test.
if __name__ == '__main__':

    # self = EmpCal([1]) # a little self.sick
    # print(self)
    # print(self.raw.__len__())
    # print(self)
    # print(self.calmonthnames[i])
    # print(self.calweeks[i])

    # print(self.calweeks[1])
    # print(self.cal['calweeks'])
    # print(self.cal.viewmonth)
    # self.cal['schoolyear'] = self._schoolyeardefault() # 2020-2021, for example
    #     self.cal['title'] = "" # ELEMENTARY PRINCIPALS, for example
    #     self.cal['workdays'] 

#     # print(sumpin.dbname)
#     # print(sumpin.cal)
#     # print(sumpin.formheaderinfo)
#     # print(sumpin.formheaderinfo)

    # cal = EmpCal([1,4,5,6,7])
    cal = EmpCal("all")
    ans = cal.datesholidsaywithfor()
    # print(ans)
    print(len(ans))

    print(cal.calnum)
    # print(cal.schoolyear)
    print(cal.title)
    print(cal.workdays)
    print(cal.calweeks)
    # print(cal.calweeks)
    # print(cal.raw)
    # print(cal.rawholidays())


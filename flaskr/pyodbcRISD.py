#!/usr/bin/python3
import pyodbc

import threading
import time
import queue
import traceback
import sys
sys.path.insert(0, '/var/www/flaskr')
from myflaskrsecrets import dbserverassessment, dbnameassessment, dbuidassessment, dbpwdassessment, dbservervisions, dbnamevisions, dbuidvisions, dbpwdvisions # , dbvyrvisions = "RISDFY2223"

linuxodbcdriver = "ODBC Driver 17 for SQL Server" # connecting to MSSQL from linux
# odbcdriver = "SQL Server" # not sure if this is always right, or needs to match ODBC 32 or 64 bit
# http://www.pymssql.org/en/stable/ref/_mssql.html not using

# from utility import Application_Logs # really should look into this more, disabled

class DBConnect():
    def __init__(self, db="assessment", timeout=8, debug=False):
        """ initialize the db connection for db={"assessment", "visions"} """
        self.server =  ""
        self.user =  ""
        self.password =  ""

        self.columns = []

        if db == "assessment":
            self.server =  dbserverassessment
            self.user =  dbuidassessment
            self.password =  dbpwdassessment
            self.db = dbnameassessment

        elif db == "visions":
            self.server =  dbservervisions
            self.user =  dbuidvisions
            self.password =  dbpwdvisions
            self.db = dbnamevisions

        connstring = 'DRIVER={{{}}};SERVER=tcp:{};DATABASE={};UID={};PWD={};Encrypt=yes;TrustServerCertificate=Yes;ssl=True;'.format(linuxodbcdriver, self.server, self.db, self.user, self.password)
        self.conn = pyodbc.connect(connstring)

        self.warn_if_sql_takes_longer_than = 25  # in seconds
        self.gather_wait_stats_for_sql_taking_longer_than = 5  # in seconds
        self.cursor = self.conn.cursor()

        self.cursor.execute('SELECT @@SPID AS SPID')
        for row in self.cursor:
            self.spid = row[0]

    def execute_i_u_d(self, sql, params=None):
        """
        Run the passed sql command for the INSERT, UPDATE or DELETE.

        Args:
            sql (str) SQL for the database to process
            params (tup) Group of params for the query

        Returns:
            res (bool) True success | False failure

        """

        try:
            sql_completion_queue = queue.Queue()
            sql_performance_monitor_thread = \
                threading.Thread(target=self.__sql_performance_monitor,
                                 args=(sql_completion_queue,
                                       sql,
                                       params,
                                       traceback.format_stack()),
                                 daemon=True)

            sql_performance_monitor_thread.start()
            # print(f"Params: {params}")
            if params:
                # print(f"pyodbcRISD HEY76: do it with params {params}")
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            ans = self.prettyupCursorObjectResults() # do this before the commmit, and after the query, if it has a return value
            self.conn.commit()
            sql_completion_queue.put("query complete")

            return ans or True # if there was any output, send it back, otherwise just True to signify done

        except Exception as e:
            sql_completion_queue.put("query complete")
            ### self.log.exception(f"Exception occurred while executing SQL: "
            ###                    f"{sql}. Params: {params}")
            return False  # e

    def execute_s(self, sql, params=None):
        """
        Execute SELECT based statement, including stored procedure.

        We filter the results down to named keys only for readability.

        Column headers are stored in class parameter self.headers

        Args:
            sql (str) SQL for the database to process.

            params (tup) list of params, if any, to map to procedure.

        Return:
            (list) list of dictionaries, else []
        """
        try:
            sql_completion_queue = queue.Queue()
            sql_performance_monitor_thread = \
                threading.Thread(target=self.__sql_performance_monitor,
                                 args=(sql_completion_queue,
                                       sql,
                                       params,
                                       traceback.format_stack()),
                                 daemon=True)

            sql_performance_monitor_thread.start()

            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)

            sql_completion_queue.put("query complete")

            return self.prettyupCursorObjectResults()

        except Exception:
            sql_completion_queue.put("query complete")
            ### self.log.exception(f"Exception occurred while executing SQL: "
            ###                    f"{sql}. Params: {params}")
            return 1

    def prettyupCursorObjectResults(self):
        ''' don't fetchall() me prematurely, convert pyodbc object to list of dictionaries '''
        # count = 0
        self.columns = []
        if self.cursor.description:
            results = []
            self.columns = [column[0] for column in self.cursor.description]
            for row in self.cursor.fetchall():
                results.append(dict(zip(self.columns, row)))
                # count += 1
            # self.record_count = count
            return results
        return False

    def __sql_performance_monitor(self, sql_completion_queue, sql, params,
                                  stack_text):
        """
        Runs as a thread when executing queries and logs when queries take too
        long.  Requires MSSQL, I think

        :param sql_completion_queue: putting anything into this queue
        notifies this function that the query is complete.
        :type sql_completion_queue: a queue.Queue() object
        :param sql: the SQL that might time out.
        :type sql: str
        :param params: parameters provided to the SQL
        :type params: tuple
        :param stack_text: a traceback showing the calls that resulted
        in this SQL being executed.
        :type stack_text: str
        :return: None
        :rtype: NoneType
        """
        wait_sql = 'EXEC msdb.dbo.sp_GetSQLWaitData @target_spid = %s'
        block_sql = 'EXEC msdb.dbo.sp_GetSQLBlockData @target_spid = %s'
        if sql == wait_sql or sql == block_sql:
            return  # Avoid recursive monitoring.

        time.sleep(self.gather_wait_stats_for_sql_taking_longer_than)
        if sql_completion_queue.qsize() != 0:
            return
        wait_stat_collection_seconds = self.warn_if_sql_takes_longer_than - \
            self.gather_wait_stats_for_sql_taking_longer_than
        monitoring_connection = DBConnect()
        for i in range(wait_stat_collection_seconds):
            wait_rows = monitoring_connection.execute_s(wait_sql, self.spid)
            execution_time = self.gather_wait_stats_for_sql_taking_longer_than\
                + i
            ### self.log.debug(f"SQL has been executing for "
            ###                f"{execution_time} seconds. SQL: {sql}, "
            ###                f"params: {params}")
            ### self.log.debug(f"Wait report: {wait_rows}")
            time.sleep(1)
            if sql_completion_queue.qsize() != 0:
                return
        ### self.log.warning(f"SQL took longer than "
        ###                  f"{self.warn_if_sql_takes_longer_than} seconds to "
        ###                  f"execute. SQL: {sql}, params: {params}, "
        ###                  f"stack: {stack_text}")
        block_rows = monitoring_connection.execute_s(block_sql, self.spid)
        ### self.log.warning(f"Block report: {block_rows}")

###########################
# manual testing...
if __name__ == '__main__':
    
    # v = DBConnect("visions")
    a = DBConnect() # default = assessment
    top_var = 1

    # sql = "select top 10 id,Description from tblPOEmpPayDeductions"
    # r = v.execute_s(sql, )
    # for rr in r:
    #     print(rr)

    # sql = "select top (?) * from tblPOEmpPayDeductions where id < ?"
    # r = v.execute_s(sql, (top_var, top_var))
    # for rr in r:
    #     print(rr)

    # # finally, get these SQL statements a bit safer...
    # sql = "select top 10 * from invdelformdetails where invid = ?"
    # r = a.execute_s(sql, "2")
    
    # for rr in r:
    #     print(f"top11: {rr}")

    # print(a.columns)

    # r = a.execute_s(sql, 1)
    # for rr in r:
    #     print(f"top12: {rr}")

    sql = """insert into invdelform
         (schoolunitdeltingitems, wonumber, notes, username, active)
         OUTPUT Inserted.id
          values (?,?,?,?,?)"""
    r = a.execute_i_u_d(sql, ('CTD - Franks Area 20230110', '', '', 'byates', 1))
    print (r)
    print(a.columns)
    # for rr in r:
    #     print(f"top226: {rr}")
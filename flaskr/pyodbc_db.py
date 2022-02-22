import pyodbc
# from pymssql import _mssql

import threading
import time
import queue
import traceback
import sys
sys.path.insert(0, '/var/www/flaskr')
sys.path.insert(0, 'C:/Users/bryany/Desktop/GitHub/flask-tutorial')
from myflaskrsecrets import dbserver, dbname, dbuid, dbpwd

# http://www.pymssql.org/en/stable/ref/_mssql.html

# from utility import Application_Logs

class MSSQL_DB_Conn():

    # def dict_factory(self, cursor, row):
    #     # d = {}
    #     # for idx, col in enumerate(cursor.description):
    #     #     d[col[0]] = row[idx]
    #     # return d

    #     # #     for row in self.cursor.fetchall():
    #     # # results.append(dict(zip(self.columns, row)))

    #     self.columns = [column[0] for column in self.cursor.description]
    #     results = []
    #     count = 0
    #     for row in self.cursor.fetchall():
    #         results.append(dict(zip(self.columns, row)))
    #         count += 1
    #     self.record_count = count
    #     return ['one potatoe','too']
    #     return results

    #         # self.columns = [column[0] for column in self.cursor.description]
    #         # results = []
    #         # count = 0
    #         # for row in self.cursor.fetchall():
    #         #     results.append(dict(zip(self.columns, row)))
    #         #     count += 1
    #         # self.record_count = count
    #         # return results




    def __init__(self, db=dbname, appname='apps', timeout=8, debug=False):
        """
        Initialize the database connection.
        Connections are closed after execution.

        Args:
            db (str)        Database to connect to

            appname (str)   Name of application initializing connection, why?

            timeout (int)   How long we'll wait for the query to complete
                            before closing connection.

            debug (bool)    If True, all queries will be printed after
                            formatting and quoting.
                            MSSQLConnection.debug_queries

        Properties:
            headers (list) Column headers if select based query.

        Returns:
            MSSQLConnection (class) Sets up the connection object for queries.

        """        
        self.server =  dbserver
        self.user =  dbuid
        self.password =  dbpwd
        self.columns = []
        # self.row_factory = self.dict_factory
        
        # linuxodbcdriver = 'FreeTDS'
        linuxodbcdriver = "ODBC Driver 17 for SQL Server"
        odbcdriver = "SQL Server" # not sure if this is always right, or needs to match ODBC 32 or 64 bit
        if self.user == "Trusted_Connection":
            self.conn = pyodbc.connect('Driver={{{}}};Server={};Database={};Trusted_Connection=yes;'.format(odbcdriver, self.server,db))
        else:
            # cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
            # cursor = cnxn.cursor()
            connstring = 'DRIVER={{{}}};SERVER=tcp:{};DATABASE={};UID={};PWD={};Encrypt=yes;TrustServerCertificate=Yes;ssl=True;'.format(linuxodbcdriver, self.server, db, self.user, self.password)
            
            # toggling one of these on/off was a pretty good test
            # connstring = 'DRIVER={{{}}};SERVER=tcp:{};DATABASE={};UID={};PWD={};Encrypt=yes;TrustServerCertificate=Yes;ssl=True;'.format(linuxodbcdriver, 'domain1', 'web', self.user, self.password)
            # connstring = 'DRIVER={{{}}};SERVER=tcp:{};DATABASE={};UID={};PWD={};Encrypt=yes;TrustServerCertificate=Yes;ssl=True;'.format(linuxodbcdriver, 'iveesql3',"RISDBase",self.user,self.password)
            
            # &Encrypt=yes
            # &TrustServerCertificate=Yes
            # &ssl=True

            self.conn = pyodbc.connect(connstring)


        # self.conn.debug_queries = debug
        ### self.log = Application_Logs().setup_logging()
        # Care should be taken to ensure this timeout is a little shorter than
        # than the worker timeout in gunicorn and the nginx gateway timeout.
        # If the worker process is killed before the SQL performance monitoring
        # thread wakes up, nothing will be logged.
        self.warn_if_sql_takes_longer_than = 25  # in seconds
        self.gather_wait_stats_for_sql_taking_longer_than = 5  # in seconds

        self.cursor = self.conn.cursor()

        self.cursor.execute('SELECT @@SPID AS SPID')
        for row in self.cursor:
            self.spid = row[0]

        
    def execute_i_u_d(self, sql, params=[]):
        """
        Run the passed sql command for the INSERT, UPDATE or DELETE.

        Args:
            sql (str) SQL for the database to process
            params (tup) Group of params for the query

        Returns:
            res (bool) True success | False failure

            ----- Rewrite for pyodbc ..... 
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
            self.cursor.execute(sql, params) 
            # linux?
            # self.cursor.execute(sql, []) # Windows
            self.conn.commit()
            sql_completion_queue.put("query complete")

            return True # "Done"

        except Exception as e:
            sql_completion_queue.put("query complete")
            ### self.log.exception(f"Exception occurred while executing SQL: "
            ###                    f"{sql}. Params: {params}")
            return False  # e

    def execute_s(self, sql, params=[], col_headers=True):
        """
        Execute SELECT based statement, including stored procedure.

        We filter the results down to named keys only for readability.

        Column headers are stored in class parameter self.headers

        Args:
            sql (str) SQL for the database to process.

            params (tup) list of params, if any, to map to procedure.

            col_headers (bool) Builds headers into a dictionary

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
            self.cursor.execute(sql, params) #  testing with params=[] as default, vs None
            # self.cursor.execute(sql, params) # fails in windows
            sql_completion_queue.put("query complete")

            self.columns = [column[0] for column in self.cursor.description]
            results = []
            count = 0
            for row in self.cursor.fetchall():
                results.append(dict(zip(self.columns, row)))
                count += 1
            self.record_count = count
            # print('maybe the count is {}'.format(count))
            return results

        except Exception:
            sql_completion_queue.put("query complete")
            ### self.log.exception(f"Exception occurred while executing SQL: "
            ###                    f"{sql}. Params: {params}")
            return 1

    def __sql_performance_monitor(self, sql_completion_queue, sql, params,
                                  stack_text):
        """
        Runs as a thread when executing queries and logs when queries take too
        long.

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
        monitoring_connection = MSSQL_DB_Conn()
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

    def row_count(self):
        """
        Number of rows affected by last query.
        For SELECT statements this value is only meaningful after reading all
        rows.

        Args:
            None

        Returns:
            res (int) Returns number of rows affected by query
        """

        try:

            return self.cursor.rows_affected

        except Exception as e:
            print(e)
            return 1

    def test_results(self):
        """
        If invoked, returns a test row with known internal data.
        In this case, we return the current year, term and offset

        Args:
            None

        Returns:
            result (dict) Returns current year and term with offset.
        """

        # sql = """SELECT academic_year, academic_term, offset
        # FROM Institute..semesterinfo
        # WHERE offset = 0"""
        sql = "select top 1 description from web..vw_web_FA_base"
        sql = "select top 1 name,type from web.sys.tables"

        try:
            r = self.cursor.execute(sql)
            return r
            # return "blue<>{}".format(r)
            # return r.__dict__

        except Exception as e:
            print(e)
            # return ("EXCEPTION:{}".format(e))

        finally:
            self.cursor.close()


###########################
#the below is a manual test.
if __name__ == '__main__':
    
    hey = MSSQL_DB_Conn()
    print(hey.user)
    print(hey.spid)
    print(hey.conn)
    sql = "select top 10 name,type from sys.tables"
    # sql = "select getdate() from sys.tables"
    r = hey.execute_s(sql)
    for rr in r:
        print(rr)
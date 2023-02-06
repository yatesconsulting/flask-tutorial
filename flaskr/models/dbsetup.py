import sys
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial/flaskr') # required for from flaskr.models.... import
sys.path.insert(0, r'C:/Users/bryany/Desktop/GitHub/flask-tutorial') # required for ?import?
# from flaskr import db
from flaskr import pyodbc_db
from myflaskrsecrets import jdbname, dbname, dbserver, tbldid, tbldip, tblxkeys, tblmerges, tblPKConstraints, tblsysProcedures

class Dbsetup():
    '''used to do things with the play database outside of dupsets;
    dbrefreshdups(), dbrefreshdek(), and dbshowlist()'''
    def __init__(self):
        self.jdbname = jdbname
        self.dbname = dbname
        self.dbserver = dbserver
        self.tbldid = tbldid
        self.tbldip = tbldip
        self.tblxkeys = tblxkeys
        self.tblmerges = tblmerges
        self.tblPKConstraints = tblPKConstraints
        self.tblsysProcedures = tblsysProcedures
        self.db = pyodbc_db.MSSQL_DB_Conn()

    def dbrefreshdups(self):
        '''refresh the dups list from namemaster, return tuple of success bool, and (flash) message'''
        sql = "delete from {}".format(self.tbldip)
        r = self.db.execute_i_u_d(sql)
        if 'error' in r and r['error']:
            return(False, "delete failed in dbsetup.py, def dbrefreshdups, sql:\n{}".format(sql))
        else:
            
            sql = f"delete from {self.tblmerges}"
            r = self.db.execute_i_u_d(sql)
            if 'error' in r and r['error']:
                return(False, "delete failed in dbsetup.py, def dbrefreshdups, sql:\n{}".format(sql))
            else:
                sql = "delete from {}".format(self.tbldid)
                r = self.db.execute_i_u_d(sql)
                if 'error' in r and r['error']:
                    return(False, "delete failed in dbsetup.py, def dbrefreshdups, sql:\n{}".format(sql))
                else:
                    sql = """
                        insert into {} 
                        (id_num, human_verified, goodid, origtablewithdup, dupset, db)
                        select
                        id_num
                        , 1 as human_verified
                        ,isnull( cast(isnull(stuff(BIRTH_NAME, 1, patindex('%[0-9]%', BIRTH_NAME)-1, ''),stuff(PREFERRED_NAME, 1, patindex('%[0-9]%', PREFERRED_NAME)-1, '')) as int),0)
                        , 'NameMaster' as origtablewithdup
                        , DER2.dupset
                        , '{jdb}'
                        from {jdb}..NameMaster NM 
                        left join 
                        (select ROW_NUMBER() OVER(ORDER BY GoodID ASC) AS dupset,
                        GoodID from (
                        select distinct 
                        isnull(cast(isnull(stuff(BIRTH_NAME, 1, patindex('%[0-9]%', BIRTH_NAME)-1, ''),stuff(PREFERRED_NAME, 1, patindex('%[0-9]%', PREFERRED_NAME)-1, '')) as int),0)
                        as GoodID
                        from {jdb}..NameMaster where BIRTH_NAME like 'dup%' or PREFERRED_NAME like 'dup%' or BIRTH_NAME like 'use%' or PREFERRED_NAME like 'use%') as DER1) as DER2
                        on DER2.goodid = 
                        isnull(  cast(isnull(stuff(BIRTH_NAME, 1, patindex('%[0-9]%', BIRTH_NAME)-1, ''),stuff(PREFERRED_NAME, 1, patindex('%[0-9]%', PREFERRED_NAME)-1, '')) as int),0)
                        where nm.BIRTH_NAME like 'dup%' or PREFERRED_NAME like 'dup%' or nm.BIRTH_NAME like 'use%' or PREFERRED_NAME like 'use%'
                    """.format(self.tbldid, jdb=self.jdbname)
                    r = self.db.execute_i_u_d(sql)
                    if 'error' in r and r['error']:
                        return(False, "insert failed in dbsetup.py, def dbrefreshdups, sql: {}".format(sql))
                    else:
                        sql = """SELECT t.name AS procedure_name
                        FROM {} AS t
                        where t.name like 'BAY_sp_dup%'""".format(self.tblsysProcedures)
                        r = self.db.execute_s(sql)
                        if not r:
                            return (False, "SQL to get list of procedures failed in dbsetup.py, def dbrefreshdups, sql:\n{}".format(sql))
                        else:
                            procedurecount = 0           
                            for a in r:
                                proc = a['procedure_name']
                                procedurecount += 1
                                # done.append(proc)
                                sql = "exec {}".format(proc)
                                r = self.db.execute_i_u_d(sql)
                                if 'error' in r and r['error']:
                                    return(False, "Procedure failed in dbsetup.py, def dbrefreshdups, sql:\n{}".format(sql))
                                else:
                                    sppart = f" and {procedurecount} stored procedure{'s' if procedurecount != 1 else ''}" if procedurecount else ""
                                    return (True, "dips, and merges cleared and dups refreshed from NameMaster{}".format(sppart))

    def dbrefreshdek(self):
        '''refresh the dups extra keys table and return tuple of success bool, and (flash) message'''
        report = ""
        sql = """
        delete from {tbl}
        
        insert into {tbl}
        (tablename,xkeys,tablekeyname,mykeyname,tableuniqkey)
        values 
        ('ORG_TRACKING',NULL,'ID_NUM','id_num','APPID') -- UI,ORG_TRACKING , SEQ_NUM
        -- changed 6/27/22 since it's now an update ('ORG_TRACKING','ORG_ID_NUM, SEQ_NUM','ID_NUM','id_num','APPID') -- UI,ORG_TRACKING , SEQ_NUM
        ,('AD_ORG_TRACKING','ORG_ID__AD','ID_NUM','id_num','APPID') -- from UniqueIndexes,AD_ORG_TRACKING, was SEQ_NUM
        -- org dups someday??? ,('ORG_TRACKING',NULL,'ORG_ID_NUM','id_num','APPID') -- UI,ORG_TRACKING , SEQ_NUM
        -- org dups someday??? ,('AD_ORG_TRACKING',NULL,'ORG_ID__AD','id_num','APPID') -- MAYBE, as id_num of orgs, not person TODO check this
        ,('ADVISING_HISTORY','SEQ_NUM','ID_NUM','id_num','APPID') -- ADVISING_HISTORY -- removed advisorid for better(?) shuffling
        ,('ADV_MASTER',NULL,'ID_NUM','id_num','APPID') -- changed per UI,ADV_MASTER
        ,('AlternateNameMasterNames',NULL,'NameMasterAppID','appid','AppID') -- inaccurate? for 48,AlternateNameMasterNames
        ,('Organization',NULL,'NameMasterAppID','appid','AppID')
        ,('Person',NULL,'NameMasterAppID','appid','AppID') -- don't see how this could matter,NameMasterAppID could maybe replace APPID as table key also,maybe,Person
        ,('STUD_AIMS',NULL,'ID_NUM','id_num','APPID') -- UI added ADV_REQ_CODE,STUD_AIMS, was AIM_LABEL,ADV_REQ_CODE
        ,('TEST_SCORES_DETAIL','TST_CDE,TST_SEQ,TST_ELEM','ID_NUM','id_num','APPID') -- UI,TEST_SCORES_DETAIL
        ,('ADVISOR_STUD_TABLE','ADVISOR_ID,DIV_CDE,SEQ_NUM','ID_NUM','id_num','APPID')
        ,('ADV_MTG_NOTES','ADVISOR_ID,MTG_DTE_TIM','ID_NUM','id_num','APPID') -- from UI,ADV_MTG_NOTES
        ,('ADV_MTG_HIST','ADVISOR_ID,MTG_DTE_TIM','ID_NUM','id_num','APPID') -- from UI,ADV_MTG_HIST
        ,('STUD_ADV_ALTER','AIM_LABEL,ADV_TREE_REQ_CDE,SEQ_NUM','ID_NUM','id_num','APPID') -- UI,STUD_ADV_ALTER
        ,('STUD_CRS_NEEDS','ADV_REQ_CODE,CLASS_CDE,REQUIRED_FLAG','ID_NUM','id_num','APPID') -- UI  added CLASS_CDE REQUIRED_FLAG,STUD_CRS_NEEDS
        --EX_PROGRAM_GPA_STUDENT_MASTER,ADV_MASTER --EX_PROGRAM_GPA_STUDENT_MASTER,ADV_MASTER
        -- skipping AlternateContactMethod_UDF,AlternateContactMethod -- skipping AlternateContactMethod_UDF,AlternateContactMethod
        ,('ADVISING_ACCESS','LOGIN_ID','ID_NUM','id_num','APPID')
        ,('ALTERNATE_IDENTIFIER','IDENTIFIER_TYPE,BEGIN_DTE,END_DTE','ID_NUM','id_num','APPID') -- was SEQ_NUM changed per UI,ALTERNATE_IDENTIFIER
        ,('CM_EMERG_CONTACTS',NULL,'ID_NUM','id_num','APPID')
        ,('AlternateContactMethod','ADDR_CDE','ID_NUM','id_num','APPID') -- from UI,AlternateContactMethod
        ,('AlternateContactMethod','ADDR_CDE','NameMasterAppID','appid','APPID')
        ,('EX_REQUIRED_GOVT_FORMS','SUBSID_CDE,GOVT_FORM_CODE','ID_NUM','id_num','APPID')
        ,('TRANS_HIST',NULL,'ID_NUM','id_num','APPID') -- UI,TRANS_HIST mess: 'SOURCE_CDE,GROUP_NUM,TRANS_KEY_LINE_NUM'
        ,('INVOICE_HEADER','INVOICE_NUM','ID_NUM','id_num','APPID') -- UI,INVOICE_HEADER
        ,('IND_YTD_BEN_HIST','BENEFIT_CDE,CALENDAR_YR','ID_NUM','id_num','APPID') -- UI,IND_YTD_BEN_HIST
        ,('IND_BEN_MAST','BENEFIT_CDE','ID_NUM','id_num','APPID') -- UI,IND_BEN_MAST
        ,('AP_MASTER','SUBSID_CDE','ID_NUM','id_num','APPID') -- UI,AP_MASTER
        ,('Archive_ADDRESS_HISTORY',NULL,'ID_NUM','id_num','APPID')
        ,('Archive_FEES_HISTORY',NULL,'ID_NUM','id_num','APPID') -- MOST LIKELY todo xkeys= mergeall or new col,Archive_FEES_HISTORY
        ,('Archive_GRADE_MAINT_HIST',NULL,'ID_NUM','id_num','SEQ_NUM') -- MOST LIKELY todo xkeys= mergeall or new col,Archive_GRADE_MAINT_HIST
        ,('ARCHIVE_NAME_HISTORY',NULL,'ID_NUM','id_num','APPID') -- MOST LIKELY todo xkeys= mergeall or new col,ARCHIVE_NAME_HISTORY
        ,('Archive_TW_API_CRP','YR_CDE,TRM_CDE,CRS_CDE','ID_NUM','id_num','APPID')
        ,('ASSET_MASTER','ASSET_NUMBER','ID_NUM','id_num','APPID') -- UI,ASSET_MASTER
        ,('ATTRIBUTE_TRANS','ATTRIB_SEQ','ID_NUM','id_num','APPID') -- UI or maybe ID_NUM,ATTRIBUTE_TRANS
        ,('AVAdviseeRosterClassificationSummary',NULL,'StudentIdNumber','id_num','ROW_ID')
        ,('PF_AWARD','POE_ID,FUND_CDE','ID_NUM','id_num','APPID') -- added APPID per UI,before CANDIDATE,PF_AWARD
        ,('PF_AWARD_TEMP','POE_ID,FUND_CDE','ID_NUM','id_num','APPID') 
        ,('BIOGRAPH_MASTER',NULL,'ID_NUM','id_num','ID_NUM') 
        ,('BIOGRAPH_HISTORY',NULL,'ID_NUM','id_num','APPID') 
        ,('Archive_BIOGRAPH_HISTORY',NULL,'ID_NUM','id_num','APPID')
        ,('CANDIDACY','YR_CDE,TRM_CDE,PROG_CDE,DIV_CDE,LOCA_CDE','ID_NUM','id_num','APPID') -- added APPID per UI before CANDIDATE,CANDIDACY
        ,('REQUIREMENTS','YR_CDE,TRM_CDE,PROG_CDE,DIV_CDE,LOC_CDE','ID_NUM','id_num','APPID') -- removed REQ_SEQ
        ,('STAGE_HISTORY_TRAN',NULL,'ID_NUM','id_num','ID_NUM,HIST_STAGE,TRANSACTION_SEQ') -- maybe before CANDIDACY and CANDIDATE,STAGE_HISTORY_TRAN
        ,('CANDIDATE',NULL,'ID_NUM','id_num','APPID') -- before STAGE_HISTORY_TRAN,CANDIDATE
        ,('CHG_CDE','CHG_CDE','ID_NUM','id_num','APPID') -- UI,CHG_CDE
        ,('CHK_HIST_HEADER','SOURCE_CDE,CHECK_BATCH_ID,CHECK_SEQNUM','ID_NUM','id_num','APPID') -- UI,CHK_HIST_HEADER
        ,('CHK_RECONCILIATION','CHECK_NUM_ALPHA,CHECK_NUM_NUMERIC','ID_NUM','id_num','APPID') -- UI,CHK_RECONCILIATION
        ,('COURSE_AUTHORIZATION','YR_CDE,TRM_CDE,CRS_CDE,SEQ_NUM','ID_NUM','id_num','APPID') -- UI,COURSE_AUTHORIZATION
        ,('coursehistory',NULL,'ID_NUM','id_num','APPID')
        --,('CUS_Tmp_FixNaming2',NULL,'ID_NUM','id_num','APPID')
        --,('CUS_TMP_update_logins',NULL,'ID_NUM','id_num','APPID')
        ,('Dataset',NULL,'ID_NUM','id_num','ID_NUM')
        ,('GRADUATION_STAGE','SEQ_NUM_2,GRAD_STAGE_CDE','ID_NUM','id_num','APPID') -- UI,GRADUATION_STAGE
        ,('DEGREE_HISTORY','SEQ_NUM_2','ID_NUM','id_num','APPID') -- todo merge shuffle SEQ_NUM_2 =UNIQ,DEGREE_HISTORY
        ,('DEGREE_HISTORY_ARCHIVE',NULL,'ID_NUM','id_num','APPID') -- MOST LIKELY todo xkeys= mergeall or same shuffle as " or new col,DEGREE_HISTORY_ARCHIVE
        ,('EMERG_CONTACT_MAST','EMER_CON_SEQ','ID_NUM','id_num','ID_NUM,EMER_CON_SEQ')
        ,('EMPL_ACA_COVERED_INDIVIDUALS','SEQ_NUM','EMPL_ID_NUM','id_num','APPID')
        ,('EMPL_MAST_UDF',NULL,'ID_NUM','id_num','APPID')
        ,('EMPL_YTD_ACA_HIST','CALENDAR_YR','ID_NUM','id_num','APPID')
        ,('IND_YTD_DIR','DDP_GRP,DDP_SEQ,CALENDAR_YR','ID_NUM','id_num','ID_NUM,DDP_GRP,DDP_SEQ,CALENDAR_YR')
        ,('IND_DIR_DEP','DDP_GRP,DDP_SEQ','ID_NUM','id_num','APPID') -- UI,IND_DIR_DEP
        ,('IND_SAL_HIST','ORG_POS,POS_SEQ,SAL_RVW_SEQ','ID_NUM','id_num','APPID') -- UI,IND_SAL_HIST
        ,('TIMCRDS','SOURCE_CDE,TIME_BATCH_ID,TIME_SEQNUM','ID_NUM','id_num','APPID') -- UI,TIMCRDS
        ,('IND_POS_HIST','ORG_POS,POS_SEQ','ID_NUM','id_num','APPID') -- UI,IND_POS_HIST
        ,('IND_YTD_TAX','TAX_CDE,CALENDAR_YR','ID_NUM','id_num','APPID') -- UI,IND_YTD_TAX
        ,('IND_TAX_MAST','TAX_CDE','ID_NUM','id_num','APPID') -- UI,IND_TAX_MAST
        ,('IND_YTD_OTH','CALENDAR_YR','ID_NUM','id_num','APPID') -- UI,IND_YTD_OTH
        ,('SUPERVIS_MAST',NULL,'ID_NUM','id_num','APPID')
        ,('EMPL_MAST',NULL,'ID_NUM','id_num','APPID')
        ,('ETHNIC_RACE_REPORT','SEQ_NUM','ID_NUM','id_num','APPID') -- UI,ETHNIC_RACE_REPORT-- TRIGGER to update _DTL
        ,('ETHNIC_REPORT_DTL','ETHNIC_RPT_DEF_NUM,SEQ_NUM','ID_NUM','id_num','APPID') -- UI,ETHNIC_REPORT_DTL
        ,('RACE_REPORT_DTL','SEQ_NUM,RACE_RPT_DEF_NUM','ID_NUM','id_num','APPID') -- UI,RACE_REPORT_DTL
        ,('FACULTY_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('FEES','SEQUENCE_NUMBER','ID_NUM','id_num','APPID') -- UI,FEES, SEQUENCE_NUMBER - > Shuffle every time
        ,('FEES_HISTORY',NULL,'ID_NUM','id_num','APPID') -- UI,FEES_HISTORY -- BATCH_NUMBER+SEQ_NUM are system wide uniq, but updating ID ok
        ,('FERPA_PERMISSION',NULL,'ID_NUM','id_num','APPID') -- UI,FERPA_PERMISSION
        ,('FERPA_PERMISSION',NULL,'PARENT_ID_NUM','id_num','APPID') -- UI,FERPA_PERMISSION
        ,('GF1098TExcludedRecords',NULL,'ID_NUM','id_num','AppID') 
        ,('GF1098TIncludedRecords',NULL,'ID_NUM','id_num','AppID')
        ,('GF1098TIssues',NULL,'ID_NUM','id_num','AppID') -- UI,GF1098TIssues
        ,('GF1098TSummary',NULL,'ID_NUM','id_num','AppID')
        ,('GRADE_MAINT_HIST',NULL,'ID_NUM','id_num','SEQ_NUM') 
        ,('HIGHEST_TEST_SCORE','TST_CDE,TST_ELEMENT','ID_NUM','id_num','APPID') 
        ,('HOLD_TRAN',NULL,'ID_NUM','id_num','APPID') 
        ,('IND_BEN_ACCRUAL_HIST',NULL,'ID_NUM','id_num','APPID') -- UI,was GROUP_NUMBER,ACTION_CODE,IND_BEN_ACCRUAL_HIST
        ,('IND_BEN_HIST','BENEFIT_CDE,IND_BENEFIT_SEQ','ID_NUM','id_num','APPID') 
        ,('IND_PAY_ACC_RATE','ORG_POS,POS_SEQ,SAL_RVW_SEQ,PAY_SEQ','ID_NUM','id_num','APPID') -- UI,IND_PAY_ACC_RATE
        ,('IPEDS_STUDENT_MAST','SNAPSHOT_DTE','ID_NUM','id_num','APPID')
        ,('IR_IPEDS_COMPLETIONS','SNAPSHOT_ID,SEQ_NUM','ID_NUM','id_num','APPID')
        ,('IR_STUDENT_DATA_UDF','SNAPSHOT_ID','ID_NUM','id_num','APPID')
        ,('IR_STUDENT_DATA','SNAPSHOT_ID','ID_NUM','id_num','APPID(notreally)')
        ,('ITEMS',NULL,'ID_NUMBER','id_num','APPID') -- UI was GROUP_NUMBER,ACTION_CODE,ITEMS, was ID_NUMBER,GROUP_NUMBER,SUBGROUP_NUMBER,GROUP_SEQUENCE
        ,('J1FormattedNames',NULL,'ID_NUM','id_num','AppID') -- UI,J1FormattedNames, was PartyAppID
        ,('LOCATION_MASTER',NULL,'CONTACT_ID_NUM','id_num','APPID') -- pretty sure 3/1/22
        --,('MCN_COHORT2009',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_COHORT2010',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_COHORT2011',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_Connect_ACL',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_DataAutomationLog',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_Dataset',NULL,'ID_NUM','id_num','IncorrectAPPID') -- UI,MCN_Dataset
        ,('MCN_REDFIELD_RESULTS',NULL,'ID_NUM','id_num','ID_NUM,RULE_NUM')
        --,('MCN_RESULTS',NULL,'ID_NUM','id_num','IncorrectAPPID')
        --,('MCN_RESULTS_12',NULL,'ID_NUM','id_num','IncorrectAPPID')
        ,('MEDIA_WRK','REC_TYPE,SEQ','ID_NUM','id_num','ID_NUM,REC_TYPE,SEQ') -- prob error ignored,MEDIA_WRK
        ,('MILITARY_SERVICE_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('ADDRESS_MASTER','ADDR_CDE','ID_NUM','id_num','AppID') -- replaced NameAddressMaster for NamePhoneMaster issues 6/14/22
        ,('NAMEPHONEMASTER','PhoneTypeDefAppID','NameMasterAppID','appid','AppID') -- first attmept at APPID,NAMEPHONEMASTER
        ,('TRANSCRIPT_REQUEST','SEQ_NUM_2','ID_NUM','id_num','APPID') -- UI,TRANSCRIPT_REQUEST
        ,('TRANSCRIPT_REQUEST',NULL,'REQUESTOR_ID_NUM','id_num','APPID') -- shuffle ignores with missing SEQ in xkeys
        ,('TRANSCRIPT_REQUEST',NULL,'SENT_TO_ID_NUM','id_num','APPID') -- "
        -- ,('NameAddressMaster','ADDR_CDE,AddressCodeDefAppID','ID_NUM','id_num','AppID') -- see ADDRESS_MASTER
        ,('NameAddressMaster','ADDR_CDE','namemasterappid','appid','AppID')

        ,('ADDRESS_HISTORY',NULL,'ID_NUM','id_num','APPID') -- todo xkeys= mergeall or new col REQUIRED_FLAG,ADDRESS_HISTORY
        ,('NEW_MERGE_ID_FILE',NULL,'ID_NUM','id_num','PROCESS_ID,LETTER_CDE,ID_NUM,GROUP_NUMBER,SUBGROUP_NUMBER,GROUP_SEQUENCE')
        ,('NSLC_EXCEPTIONS',NULL,'ID_NUM','id_num','APPID')
        ,('NSLC_REG_TRANSACT','NSL_YR_CDE,NSL_TRM_CDE,NSL_SEQ_NUM','NSL_ID_NUM','id_num','APPID') --UI mentions NSL_NSL_YR_CDE prob error ignored,NSLC_REG_TRANSACT
        ,('NSLC_STUDENT_MAST',NULL,'NSL_ID_NUM','id_num','NSL_ID_NUM')
        ,('NSLC_TRANS_HISTORY','NSL_FICE_CDE,NSL_BRANCH_CDE,NSL_ACAD_TERM','NSL_ID_NUM','id_num','NSL_ID_NUM')-- can't serialize dates, NSL_ACAD_TERM should be NSL_REPORT_DTE
        ,('ONLINE_PAYMENT',NULL,'ID_NUM','id_num','APPID')
        ,('ORG_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('PA_WRK_STDY_EDIT',NULL,'ID_NUM','id_num','APPID') -- todo xkeys= mergeall or new col,PA_WRK_STDY_EDIT
        ,('PF_APPLICANT_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('PF_DIRECT_LOAN_RPT',NULL,'ID_NUM','id_num','APPID')
        ,('PF_DISB_TRANS_HIST',NULL,'ID_NUM','id_num','SEQ_NUM')
        ,('PF_ERROR_LOG',NULL,'ID_NUM','id_num','APPID')
        ,('PF_SAP_HIST','YR_CDE,TRM_CDE','ID_NUM','id_num','APPID') -- UI,PF_SAP_HIST
        ,('PF_STDNT_AWARD','POE_ID,FUND_CDE,RELEASE_STS','ID_NUM','id_num','APPID')
        ,('PF_STDNT_EMP','FUND_CDE,POE_ID,WRK_STDY_DEPT','ID_NUM','id_num','APPID')
        ,('PF_STDNT_MSTR','POE_ID','ID_NUM','id_num','APPID') -- UI,PF_STDNT_MSTR
        ,('PF_WRK_STDY','POE_ID,FUND_CDE,WRK_STDY_DEPT','ID_NUM','id_num','APPID')
        ,('PO_HEADER','GRP_NUM','ID_NUM','id_num','APPID') -- UI,PO_HEADER
        ,('PO_HEADER',NULL,'alt_req_id_num','id_num','APPID') -- alt_req_id_num fix
        ,('REQ_HEADER',NULL,'ALT_REQ_ID_NUM','id_num','APPID') -- added with Larry
        ,('Query',NULL,'ID_NUM','id_num','APPID')
        ,('RECEIPT_HEADER',NULL,'ID_NUM','id_num','APPID') -- UI-- really?,RECEIPT_HEADER
        ,('RELATION_TABLE','REL_TABLE_SEQ','ID_NUM','id_num','APPID') -- UI,RELATION_TABLE
        ,('RELATION_TABLE',NULL,'REL_ID_NUM','id_num','APPID')
        ,('REQ_HEADER','GRP_NUM','ID_NUM','id_num','APPID') -- UI,REQ_HEADER
        ,('SEVIS_STU_FINANCIAL',NULL,'ID_NUM','id_num','APPID') -- STUD_SEQ_NUM,BATCH_NUMBER,SEVIS_STU_FINANCIAL
        ,('SEVIS_STUDENT',NULL,'ID_NUM','id_num','APPID')
        ,('SEVIS_GENERAL',NULL,'ID_NUM','id_num','APPID')
        ,('SPORTS_TRACKING','YR_CDE,TRM_CDE,SPORTS_CDE','ID_NUM','id_num','APPID') -- UI,SPORTS_TRACKING
        ,('STUD_AIMS_TEMP','AIM_LABEL,ADV_REQ_CODE','ID_NUM','id_num','APPID')
        ,('STUD_HOURS_CRS',NULL,'ID_NUM','id_num','APPID') -- 'YR,TRM' 3/31/22 removed since recalc will fix
        ,('STUD_STERM_SUM_DIV',NULL,'ID_NUM','id_num','ID_NUM,DIV_CDE,YR_CDE,TRM_CDE,SUBTERM_CDE') -- 'DIV_CDE,YR_CDE,TRM_CDE,SUBTERM_CDE' 3/31/22 removed since recalc will fix
        ,('STUD_TERM_SUM_DIV',NULL,'ID_NUM','id_num','APPID') -- UI,STUD_TERM_SUM_DIV, 'DIV_CDE,YR_CDE,TRM_CDE' 3/31/22 removed since recalc will fix 
        -- when trying to shuffle these, they looked like this
        --,('STUDENT_CRS_HIST_PFLAG_HIST','YR_CDE,TRM_CDE,STUD_SEQ_NUM','ID_NUM','id_num','APPID') -- UI,but?,STUD_SEQ_NUM,BATCH_NUMBER,STUDENT_CRS_HIST_PFLAG_HIST, 'YR_CDE,TRM_CDE'  3/31/22 removed since recalc will fix
        --,('STUDENT_CRS_HIST','YR_CDE,TRM_CDE,STUD_SEQ_NUM','ID_NUM','id_num','APPID') -- UI ?,STUD_SEQ_NUM,STUDENT_CRS_HIST
        -- trying to delete them, let's try them this way
        ,('STUDENT_CRS_HIST_PFLAG_HIST',NULL,'ID_NUM','id_num','APPID') -- UI,but?,STUD_SEQ_NUM,BATCH_NUMBER,STUDENT_CRS_HIST_PFLAG_HIST, 'YR_CDE,TRM_CDE'  3/31/22 removed since recalc will fix
        ,('STUDENT_CRS_HIST',NULL,'ID_NUM','id_num','APPID') -- UI ?,STUD_SEQ_NUM,STUDENT_CRS_HIST
        ,('STUDENT_DIV_MAST','DIV_CDE','ID_NUM','id_num','APPID') -- UI,STUDENT_DIV_MAST
        ,('STUDENT_REG_ALLOWED_LOC','LOC_CDE','ID_NUM','id_num','APPID')
        ,('STUDENT_MASTER_EXT',NULL,'ID_NUM','id_num','APPID')
        ,('STUDENT_MIDTRM_SUM',NULL,'ID_NUM','id_num','ID_NUM,DIV_CDE,YR_CDE,TRM_CDE') -- 'DIV_CDE,YR_CDE,TRM_CDE' 3/31/22 removed since recalc will fix 
        ,('STUDENT_TERM_SUM',NULL,'ID_NUM','id_num','APPID') -- UI,STUDENT_TERM_SUM, 'YR_CDE,TRM_CDE' 3/31/22 removed since recalc will fix 
        ,('STUDENT_TERM_TABLE','YR_CDE,TRM_CDE','ID_NUM','id_num','APPID') -- UI,STUDENT_TERM_TABLE
        ,('TRANSCRIPT_NOTE','DIV_CDE,YR_CDE,TRM_CDE,SEQ_NUM_2','ID_NUM','id_num','APPID')
        ,('TRANSCRIPT_HEADER',NULL,'ID_NUM','id_num','ID_NUM,DIV_CDE,YR_CDE,TRM_CDE')
        ,('STUDENT_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('STUDENT_PROGRESS','AIM_LABEL,ADV_TREE_REQ_CDE,SEQ_NUM','ID_NUM','id_num','APPID') -- UI,STUDENT_PROGRESS
        ,('STUDENT_PROGRESS_TEMP','AIM_LABEL,ADV_TREE_REQ_CDE,SEQ_NUM','ID_NUM','id_num','APPID') -- UI,STUDENT_PROGRESS_TEMP
        ,('STUDENT_YR_REPORT','YR','ID_NUM','id_num','APPID') -- UI,STUDENT_YR_REPORT
        ,('SUBMISSION',NULL,'SUBMITTER_ID','id_num','APPID')
        ,('SUBMISSION_HISTORY','SEQ_NUM','ID_NUM','id_num','APPID')
        ,('SUBSID_MASTER','SUBSID_CDE','ID_NUM','id_num','APPID') -- UI,SUBSID_MASTER
        ,('SUBSID_RPT','SUBSID_CDE,LOCK_JOB_NUM,USER_NAME','ID_NUM','id_num','APPID') -- UI,SUBSID_RPT
        ,('TEST_SCORES_UDF','TST_CDE,TST_SEQ','ID_NUM','id_num','APPID')
        ,('TEST_SCORES','TST_CDE,TST_SEQ','ID_NUM','id_num','APPID') -- UI,TEST_SCORES
        ,('trans_temp',NULL,'ID_NUM','id_num','APPID')
        ,('TW_API_CRP','YR_CDE,TRM_CDE,CRS_CDE','ID_NUM','id_num','ID_NUM,YR_CDE,TRM_CDE,CRS_CDE')
        ,('TW_API_CST','JICS_GROUP_ID','ID_NUM','id_num','ID_NUM,JICS_GROUP_ID')
        ,('TW_API_PRS',NULL,'ID_NUM','id_num','ID_NUM')
        ,('TW_GRP_MEMBERSHIP','GROUP_ID','ID_NUM','id_num','ID_NUM,GROUP_ID')
        ,('TW_WEB_SECURITY',NULL,'ID_NUM','id_num','AppID') -- UI,TW_WEB_SECURITY
        ,('VENDOR_WEB_SITE','URL','ID_NUM','id_num','APPID')
        ,('VNDR_MASTER',NULL,'ID_NUM','id_num','APPID')
        ,('NameMaster',NULL,'ID_NUM','id_num','APPID')
        ,('NAME_HISTORY',NULL,'ID_NUM','id_num','APPID')        

        update DEK
        set tableuniqkey = replace(P.cols,' ','') 
        from {tbl} DEK
        join {con} P
        on P.tablename = DEK.tablename
        
        update {tbl} --somehow this is actually really correct
        set tableuniqkey = 'APPID'
        where tablename = 'CANDIDACY'

        update {tbl}
        set defaultaction = 'shuffle'
        where xkeys like '%seq%' and tablename in ( -- not quite making sure seqWhatever is the last thing in the list, but we'll call it good
        'NSLC_REG_TRANSACT'
        --, 'ORG_TRACKING' -- most of the time this works, maybe not for orgs, and unsure of AD_ORG_TRACKING involvement, failing moved to update
        --, 'DEGREE_HISTORY' -- DEGREE_HISTORY -> GRADUATION_STAGE in a way that makes this need to be deleted
        , 'RACE_REPORT_DTL', 'COURSE_AUTHORIZATION'
        --, 'REQUIREMENTS'  -- I cannot decide on this one
        , 'STUD_ADV_ALTER', 'ADVISING_HISTORY', 'ATTRIBUTE_TRANS', 'ADVISOR_STUD_TABLE'
        , 'TEST_SCORES', 'TEST_SCORES_DETAIL' -- maybe not, looking at 4366423 ???
        , 'TRANSCRIPT_REQUEST', 'FEES', 'STUDENT_PROGRESS', 'RELATION_TABLE', 'ETHNIC_REPORT_DTL'
        )

        update {tbl}
        set defaultaction = 'update' -- many of the histories can probably go here
        where tablename in (
            'ORG_TRACKING' -- failing in shuffle sometimes, maybe the enddate needs tweaking
            ,'ETHNIC_RACE_REPORT'
            ,'NAME_HISTORY' -- definately correct
            ,'Archive_ADDRESS_HISTORY'
            ,'HOLD_TRAN' -- pretty sure, except the DUP hold TODO
            ,'FERPA_PERMISSION' -- works on PARENT_ID_NUM, hopefully ID_NUM also
            -- ,'EMPL_MAST' -- nope
        )    

        update {tbl}
        set defaultaction = 'delete'
        where tablename in (
        'J1FormattedNames'
        ,'STUDENT_CRS_HIST','STUDENT_CRS_HIST_PFLAG_HIST'
        ,'DEGREE_HISTORY' -- DEGREE_HISTORY -> GRADUATION_STAGE in a way that makes this need to be deleted
        ,'AlternateNameMasterNames'
        ,'AlternateContactMethod'
        )

        """.format(tbl=self.tblxkeys, con=self.tblPKConstraints )
        r = self.db.execute_i_u_d(sql)
        if 'error' in r and r['error']:
            msg = "dekrefresh FAILED - check huge sql in dbsetup.py, def dbrefreshdek, it uses tbl={} and con={}".format(self.tblxkeys, self.tblPKConstraints)
            return (False, msg)
        else:
            return (True, "Refresh of DupExtraKeys success")
            
    def dbshowlist(self):
        '''return tuple of success bool, (flash) message, and list of rows for display of dupsets'''
        sql = "select distinct db from {}".format(self.tbldid)
        r = self.db.execute_s(sql)
        if self.db.record_count == 0:
            msg = "Please build the dup list first"
            return (False, msg,)
        elif self.db.record_count > 1:
            msg = "Something is wrong, please rebuild the dup list"
            return (False, msg,)
        else:
            msg = "using jdbase {}".format(r[0]['db'])
            sql = """ 
            select dupset,D.id_num,human_verified,goodid,origtablewithdup,db,birth_name,preferred_name
            ,N.LAST_NAME + ', ' + N.FIRST_NAME as LastFirst
            from {did} D
            join {jdb}..namemaster N
            on n.ID_NUM = D.id_num
            union
            select dupset,goodid,human_verified,goodid,origtablewithdup,db,birth_name,preferred_name
            ,N.LAST_NAME + ', ' + N.FIRST_NAME as LastFirst
            from {did} D
            join {jdb}..namemaster N
            on n.ID_NUM = D.goodid
            where isnull(d.goodid, 0)>0""".format(jdb=self.jdbname,did=self.tbldid)
            r = self.db.execute_s(sql)
            return (True, msg, r)

if __name__ == '__main__':
    t = Dbsetup()
    # this is the best order to do a basic refresh, and 
    success, msg = t.dbrefreshdek()
    topnum = 3
    if success:
        print ("q1 worked with msg {}".format(msg))
        success, msg = t.dbrefreshdups()
        if success:
            print ("q2 worked with msg {}".format(msg))
            success, msg, r = t.dbshowlist()
            if success:
                print ("q3 worked with msg {}".format(msg))
                print("all refreshes worked, {} dupsets, and here's your top {}:".format(len(r), topnum))
                for rr in range(topnum):
                    print("row {}:{}".format(rr+1, r[rr]))
            else:
                print("fail: {}".format(msg))
        else:
            print("fail: {}".format(msg))
    else:
        print("fail: {}".format(msg))
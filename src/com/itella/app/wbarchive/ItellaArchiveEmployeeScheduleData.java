package com.itella.app.wbarchive;


import java.sql.SQLException;
import com.workbrain.app.wbarchive.WBArchive;
import com.workbrain.app.wbarchive.WBArchiveException;

/**
 * Extension class for archiving employee schedule data.
 * The duplicate records in archive instance needs to be purged before running policy.
 * The duplicate records in core instance are formed due to Itella importing records prior to Hands Off Date
 * 
 */
public class ItellaArchiveEmployeeScheduleData extends AbtractItellaArchiveData {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ItellaArchiveEmployeeScheduleData.class);

    private static final String SQL_DELETE_EMP_SCHD_DTL_LAYER = "delete from  ARCHIVE.EMP_SCHD_DTL_LAYER WHERE (emp_id, eschdl_work_date) IN (SELECT emp_id, eschdl_work_date FROM workbrain.EMP_SCHD_DTL_LAYER)";
    private static final String SQL_DELETE_EMPLOYEE_SCHED_DTL = "delete from  ARCHIVE.EMPLOYEE_SCHED_DTL WHERE (emp_id, eschd_work_date) IN (SELECT emp_id, eschd_work_date FROM workbrain.EMPLOYEE_SCHED_DTL)";
    private static final String SQL_DELETE_EMPLOYEE_SCHEDULE = "delete from  ARCHIVE.EMPLOYEE_SCHEDULE WHERE (emp_id, work_date) IN (SELECT emp_id, work_date FROM workbrain.EMPLOYEE_SCHEDULE )";
    
    @Override
    public void processArchive(WBArchive wbarchive) throws WBArchiveException , SQLException{
    	int cnt;
    	cnt = executeSQL(wbarchive.getDBConnectionCore(), SQL_DELETE_EMP_SCHD_DTL_LAYER);
    	if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) logger.debug("Purged " + cnt + " EMP_SCHD_DTL_LAYER records");
    	wbarchive.getDBConnectionCore().commit();
    	
    	cnt = executeSQL(wbarchive.getDBConnectionCore(), SQL_DELETE_EMPLOYEE_SCHED_DTL);
    	if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) logger.debug("Purged " + cnt + " EMPLOYEE_SCHED_DTL records");
    	wbarchive.getDBConnectionCore().commit();
    	
    	cnt = executeSQL(wbarchive.getDBConnectionCore(), SQL_DELETE_EMPLOYEE_SCHEDULE);
    	if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) logger.debug("Purged " + cnt + " EMPLOYEE_SCHEDULE records");
    	wbarchive.getDBConnectionCore().commit();
    	
    	wbarchive.appendTransactionMessage(". Purged " + cnt + " EMPLOYEE_SCHEDULE records from archive instance prior to run.");
    	
    	super.processArchive(wbarchive);
    }


}

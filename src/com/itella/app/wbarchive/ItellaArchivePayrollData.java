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
public class ItellaArchivePayrollData extends AbtractItellaArchiveData {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ItellaArchivePayrollData.class);

    private static final String SQL_DELETE_work_detail_adjust = "delete from archive.work_detail_adjust  where (emp_id, wrkda_work_date) in    ( " +
                                                        " select emp_id, wrkda_work_date " +
                                                        " from workbrain.work_detail_adjust )";
    
    private static final String SQL_DELETE_employee_balance_log = "delete from archive.employee_balance_log where (emp_id, wrks_work_date) in (SELECT emp_id, wrks_work_date FROM workbrain.employee_balance_log) ";
    private static final String SQL_DELETE_clock_tran_processed = "delete from archive.clock_tran_processed where archive.clock_tran_processed.wrks_id in " +
        "(       select ar.wrks_id "  + 
        "        from workbrain.work_summary wb, archive.work_summary ar "  + 
        "        where wb.emp_id = ar.emp_id and wb.wrks_work_date = ar.wrks_work_date)";
    
    
    private static final String SQL_DELETE_work_detail = "      delete from  ARCHIVE.work_detail WHERE (wrks_id) IN ( "+
                                                " select ar.wrks_id "+
                                                " from workbrain.work_summary wb, archive.work_summary ar "+ 
                                                " where wb.emp_id = ar.emp_id "+
                                                " and wb.wrks_work_date = ar.wrks_work_date)";
    private static final String SQL_DELETE_work_summary = "delete from  ARCHIVE.work_summary WHERE (emp_id, wrks_work_date) IN (SELECT emp_id, wrks_work_date FROM workbrain.work_summary)";    
    
    public void processArchive(WBArchive wbarchive) throws WBArchiveException , SQLException{
    	int cnt;
    	cnt = executeSQL(wbarchive.getDBConnectionCore(), SQL_DELETE_work_detail_adjust);
    	if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) logger.debug("Purged " + cnt + " EMP_SCHD_DTL_LAYER records");
    	wbarchive.getDBConnectionCore().commit();
    	
    	cnt = executeSQL(wbarchive.getDBConnectionCore(), SQL_DELETE_employee_balance_log);
    	if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) logger.debug("Purged " + cnt + " employee_balance_log records");
    	wbarchive.getDBConnectionCore().commit();
    	
    	cnt = executeSQL(wbarchive.getDBConnectionCore(), SQL_DELETE_clock_tran_processed);
    	if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) logger.debug("Purged " + cnt + " clock_tran_processed records");
    	wbarchive.getDBConnectionCore().commit();
    	
    	cnt = executeSQL(wbarchive.getDBConnectionCore(), SQL_DELETE_work_detail);
    	if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) logger.debug("Purged " + cnt + " work_detail records");
    	wbarchive.getDBConnectionCore().commit();
    	
    	cnt = executeSQL(wbarchive.getDBConnectionCore(), SQL_DELETE_work_summary);
    	if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) logger.debug("Purged " + cnt + " work_summary records");
    	wbarchive.getDBConnectionCore().commit();
    	
    	wbarchive.appendTransactionMessage(". Purged " + cnt + " WORK_SUMMARY records from archive instance prior to run.");
    	super.processArchive(wbarchive);
    	
    }

}

package com.itella.app.wbarchive;


import java.sql.SQLException;
import com.workbrain.app.wbarchive.WBArchive;
import com.workbrain.app.wbarchive.WBArchiveException;

/**
 * Extension class for archiving prod comp data.
 * The duplicate records in archive instance needs to be purged before running policy.
 * 
 */
public class ItellaArchiveProdCompData extends AbtractItellaArchiveData {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ItellaArchiveProdCompData.class);

    private static final String SQL_DELETE_PROD_COMP = "delete from archive.itel_prodcomp WHERE (ipg_id, ipc_date)  IN (SELECT ipg_id, ipc_date FROM itel_prodcomp)";
    
    @Override
    public void processArchive(WBArchive wbarchive) throws WBArchiveException , SQLException{
    	int cnt;
    	cnt = executeSQL(wbarchive.getDBConnectionCore(), SQL_DELETE_PROD_COMP);
    	if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) logger.debug("Purged " + cnt + " ITEL_PRODCOMP records");
    	wbarchive.getDBConnectionCore().commit();
    	   	
    	wbarchive.appendTransactionMessage(". Purged " + cnt + " ITEL_PRODCOMP records from archive instance prior to run.");
    	
    	super.processArchive(wbarchive);
    }


}

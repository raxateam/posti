/*
  ***************************************************************
  *                                                             *
  *                           NOTICE                            *
  *                                                             *
  *   THIS SOFTWARE IS THE PROPERTY OF AND CONTAINS             *
  *   CONFIDENTIAL INFORMATION OF INFOR AND/OR ITS AFFILIATES   *
  *   OR SUBSIDIARIES AND SHALL NOT BE DISCLOSED WITHOUT PRIOR  *
  *   WRITTEN PERMISSION. LICENSED CUSTOMERS MAY COPY AND       *
  *   ADAPT THIS SOFTWARE FOR THEIR OWN USE IN ACCORDANCE WITH  *
  *   THE TERMS OF THEIR SOFTWARE LICENSE AGREEMENT.            *
  *   ALL OTHER RIGHTS RESERVED.                                *
  *                                                             *
  *   (c) COPYRIGHT 2016 INFOR.  ALL RIGHTS RESERVED.           *
  *   THE WORD AND DESIGN MARKS SET FORTH HEREIN ARE            *
  *   TRADEMARKS AND/OR REGISTERED TRADEMARKS OF INFOR          *
  *   AND/OR ITS AFFILIATES AND SUBSIDIARIES. ALL RIGHTS        *
  *   RESERVED.  ALL OTHER TRADEMARKS LISTED HEREIN ARE         *
  *   THE PROPERTY OF THEIR RESPECTIVE OWNERS.                  *
  *                                                             *
  ***************************************************************
 */

package com.itella.app.wbarchive;

import java.util.Map;

import com.workbrain.app.scheduler.enterprise.AbstractScheduledJob;
import com.workbrain.app.scheduler.enterprise.ScheduledJob;
import com.workbrain.app.ta.db.CodeMapper;
import com.workbrain.app.wbarchive.WBArchiveContext;
import com.workbrain.app.wbarchive.WBArchiveException;
import com.workbrain.util.StringUtil;
import com.workbrain.server.registry.Registry;

/**
 * Task class for processing policies.
 */
public class WBArchivePurgeProcessTask extends com.workbrain.app.wbarchive.WBArchiveProcessTask
{
    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(WBArchivePurgeProcessTask.class);

    public final static String ARCHIVE_POLICY_NAMES_PARAM = "ARCHIVE_POLICY_NAMES";
    public final static String AGING_DAYS_PARAM = "AGING_DAYS";
    public final static String AGING_DATE_PARAM = "AGING_DATE";
    private static final String WBREG_ARCHIVE_POLICY_AGING_DAYS = "/system/archiving/ARCHIVE_POLICY_AGING_DAYS";
    public final static String ALL = "ALL";

	public final static String AGING_SELECTION_PARAM = "AGING";

    private StringBuffer taskLogMessage = new StringBuffer( "Scheduled OK" );

    public ScheduledJob.Status run(long taskID, Map<String,Object> parameters) throws Exception {

        // *** prepare the WBArchiveContext
        WBArchiveContext context = new WBArchiveContext();
        try {
            context.assignDefaultContextValues();

            //com.workbrain.app.wbarchive.WBArchiveProcessTask tsk = (com.workbrain.app.wbarchive.WBArchiveProcessTask) this;
            WBArchiveProcess arp = new WBArchiveProcess(context, this);
            // *** archive policies
            String sPols = (String) parameters.get(ARCHIVE_POLICY_NAMES_PARAM);
            String sPolsFinal = StringUtil.isEmpty(sPols) || ALL.equals(sPols) ? null :
                sPols;
            int agingDays = 0 ;
            String agingDaysStr = (String) parameters.get(AGING_DAYS_PARAM);
            if(StringUtil.isEmpty(agingDaysStr)) {
            	agingDays = Registry.getVarInt(WBREG_ARCHIVE_POLICY_AGING_DAYS, 99999);
            }
            else {
            	agingDays = Integer.parseInt(agingDaysStr);
            }
            arp.setAgingDays(agingDays);
            
            String agingDate = (String) parameters.get(AGING_DATE_PARAM);
            arp.setAgingDate(agingDate);
            
            String agingSelection = (String) parameters.get(AGING_SELECTION_PARAM);
            
            arp.setAgingSelection(agingSelection);

            arp.setArchivePolicyNamesToRun(sPolsFinal);
            appendToTaskLogMessage ("Processing policies : " + (StringUtil.isEmpty(sPolsFinal) ? ALL : sPolsFinal));

            CodeMapper.createCodeMapper(context.getConnectionCore());
            arp.process();
            if(this.isInterrupted()){
            	return jobInterrupted("Archive policy  task has been interrupted.");
            }
            // update the boundary date
            arp.updateBoundaryDate();

            if (arp.hasArchivingErrors()) { 
        		throw new WBArchiveException("Errors occurred while running archiving policies. Please check archiving transaction log for details.");
        	}            
        }
        catch (Exception e) {
            if (logger.isEnabledFor(org.apache.log4j.Level.ERROR)) {
                logger.error("Error in archiving task", e);
            }
            throw e;
        }
        finally {
            if (context != null) {
               if (context.getConnectionArchive() != null) {
                   try {
                       context.getConnectionArchive().commit();
                   }
                   catch(Exception ex){
                       if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG))
                            logger.debug("Error when commit the transaction", ex);
                   }
                   finally {
                       context.getConnectionArchive().close();
                   }
               }
               if (context.getConnectionCore() != null) {
                   try {
                       context.getConnectionCore().commit();
                   }
                   catch(Exception ex){
                       if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG))
                            logger.debug("Error when commit the transaction", ex);
                   }
                   finally {
                       context.getConnectionCore().close();
                   }
               }
            }
        }
        return jobOk(taskLogMessage.toString());


    }

    public String getTaskUI() {
        return "/jobs/wbarchive/wbarchiveParameters.jsp";
    }

    protected void appendToTaskLogMessage(String s) {
        taskLogMessage.append("<br>" + s);
    }


}
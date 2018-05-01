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
  *   (c) COPYRIGHT 2015 INFOR.  ALL RIGHTS RESERVED.           *
  *   THE WORD AND DESIGN MARKS SET FORTH HEREIN ARE            *
  *   TRADEMARKS AND/OR REGISTERED TRADEMARKS OF INFOR          *
  *   AND/OR ITS AFFILIATES AND SUBSIDIARIES. ALL RIGHTS        *
  *   RESERVED.  ALL OTHER TRADEMARKS LISTED HEREIN ARE         *
  *   THE PROPERTY OF THEIR RESPECTIVE OWNERS.                  *
  *                                                             *
  ***************************************************************
 */

package com.itella.app.wbarchive;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.naming.NamingException;

import com.workbrain.app.wbarchive.*;
import com.workbrain.app.wbarchive.WBArchiveProcessTask;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.workbrain.app.wbarchive.db.WBArchPolicyAccess;
import com.workbrain.app.wbarchive.model.WBArchPolicyData;
import com.workbrain.app.wbarchive.model.WBArchTransData;
import com.workbrain.server.registry.Registry;
import com.workbrain.sql.DBConnection;
import com.workbrain.sql.SQLTestUtil;
import com.workbrain.util.DateHelper;
import com.workbrain.util.JavaUtil;
import com.workbrain.util.RegistryHelper;
import com.workbrain.util.StringUtil;
import com.workbrain.util.WorkbrainSystem;


//Main Archive Process Task


/**
 * Pocessor class for archive policies.
 */
public class WBArchiveProcess {
    private static final Logger logger = Logger.getLogger(WBArchiveProcess.class);
     
    private static final String WBREG_ARCHIVE_POLICY_AGING_DAYS = "/system/archiving/ARCHIVE_POLICY_AGING_DAYS";
    private static final String ARCHIVE_BOUNDARY_DATE = "/system/archiving/ARCHIVE_BOUNDARY_DATE";
   
    private WBArchiveContext context;
    private List<WBArchTransData> transactions;
    private String archivePolicyNamesToRun;
    
    /** true if there is a least one archive policy enabled */
    private boolean archiveEnabledPolicyExists = false;
    
    /** set to true if at least one of the archiving policies fail */
    private boolean archivingErrorOccured = false;
    
    /** the boundary date to be used for ARCHIVE_BOUNDARY_DATE */
	private Date boundaryDate;
	
	/** the number of days to go back from current date to determine what should be archived */
	private int agingDays = 99999;
	
	/** the date from which should be archived */
	private String agingDate = "";
	
	/** the number of days to go back from current date or the actual date to determine what should be archived */
	private String agingSelection = "";
	
    private WBArchiveProcessTask archiveTask = null;
	
    public WBArchiveProcess(WBArchiveContext context) {
        this(context, null);
    }

    public WBArchiveProcess(WBArchiveContext context, WBArchiveProcessTask archiveTask) {
        this.context = context;
        transactions = new ArrayList<WBArchTransData>();
        this.archiveTask = archiveTask;
    }

    public void setArchivePolicyNamesToRun(String v){
        archivePolicyNamesToRun=v;
    }
    
    public void setAgingDays(int v){
        this.agingDays=v;
    }
    
    public int getAgingDays() {
    	return this.agingDays;
    }

    /**
     * Processes archive policies for given WBArchiveContext
     *
     * @throws WBArchiveException
     * @throws SQLException
     */
    public void process() throws WBArchiveException , SQLException{
        List<WBArchPolicyData> arch = WBArchPolicyAccess.loadArchivePolicyByNames(context.
            getConnectionCore(),
            StringUtil.detokenizeString(archivePolicyNamesToRun, ","));

        if (arch == null || arch.size() == 0) {
            log ("No archive policy definitions found for policy names : "  + archivePolicyNamesToRun);
            return;
        }
        Iterator<WBArchPolicyData> iter1 = arch.iterator();
        //*** go through all enabled archive definitions
        while (iter1.hasNext()) {
            WBArchPolicyData ard = iter1.next();
            boolean isArchiveEnabledPolicy = false;
            if (ard.isArchive()) {
            	isArchiveEnabledPolicy = ard.isEnabled();
            	if (isArchiveEnabledPolicy) {
            		int agingDaysCalc = 0;
            		if (!archiveEnabledPolicyExists) {
            			archiveEnabledPolicyExists = true;
            		}
            			//agingDays = Registry.getVarInt(WBREG_ARCHIVE_POLICY_AGING_DAYS, isGlobal);
            			
            			String agingSelection = getAgingSelection();
            			
            			
            			if ("DATE".equals(agingSelection)) {
            			  //if aging date is empty assign default value from aging day registry paramter .
            			  // There is the case where  date parameter is selected but date picker is empty. 
            				if(!StringUtil.isEmpty(agingDate)) {
            					java.util.Date inputDate = DateHelper.convertStringToDate(agingDate,"yyyyMMdd hhmmss");
            					java.util.Date currentDate = new java.util.Date();

            					long diffInSecs = DateHelper.getHoursBetween(currentDate, inputDate);
            					long diffIndays = diffInSecs / DateHelper.HOURS_PER_DAY;

            					agingDaysCalc = Integer.parseInt(String.valueOf(diffIndays));
            				}
            				else {
            					agingDaysCalc = Registry.getVarInt(WBREG_ARCHIVE_POLICY_AGING_DAYS, 99999);
            				}

            			} else {
            				agingDaysCalc = getAgingDays();
            			}
            			boundaryDate = DateHelper.addDays(DateHelper.getCurrentDate() , -1 * agingDaysCalc);        		
            			// make sure earliest date is 01/01/1900
            			if (boundaryDate.before(DateHelper.DATE_1900)) {
            				boundaryDate = DateHelper.DATE_1900;
            			}
            		
            		ard.setWbapAgingDays(agingDaysCalc);
            	}          	
            }
            
            if (!ard.isEnabled()) {
            	log ("Archive definition : " + ard.getWbapName() + " is disabled");
                continue;
            }
            
            long st = System.currentTimeMillis();
            WBArchive wbarchive = new WBArchive(context , ard, archiveTask);
            WBPurge wbpurge = new WBPurge(context , ard, archiveTask);
            try {
            	wbarchive.loadPolicy();
                wbpurge.loadPolicy();
                wbarchive.createArchiveTransaction(true);

                String cls = ard.getWbapClass();
                WBArchiveComponent wba = (WBArchiveComponent)Class.forName(cls).newInstance();

                if (!wba.isTypeArchiveImplemented() && ard.isArchive()) {
                    wbarchive.errorArchiveTransaction("Archive policy is of ARCHIVE type but"
                        + " implementing class does not perform archiving for policy"
                        + ard.getWbapName());
                    continue;
                }
                if (!wba.isTypePurgeImplemented() && ard.isPurge()) {
                    wbarchive.errorArchiveTransaction("Archive policy is of PURGE type but"
                        + " implementing class does not perform purging for policy"
                        + ard.getWbapName());
                    continue;
                }

                log("Starting to process archive definition : " + ard.getWbapName());
                if (archiveTask != null){
                    if(archiveTask.isInterrupted()){
                    	wbarchive.rollbackWork();
                    	break;
                    }
                }
                wba.processArchive(wbarchive);
                processArchiveSchemaPurge(wbpurge);

                log(wbarchive.getTransactionMessage());
                meterTime("Processing archive definition : " + ard.getWbapName(), st);

                wbarchive.finalizeArchiveTransaction(wbarchive.getTransactionMessage());
                wbarchive.notifyUsers();
                wbarchive.commitWork();

            } catch (ClassNotFoundException e) {
            	if (isArchiveEnabledPolicy) {
            		archivingErrorOccured  = true;
            	}
                wbarchive.errorArchiveTransaction("Archive Class not found\n" + JavaUtil.getStackTrace(e));
            } catch (Throwable e) {
            	if (isArchiveEnabledPolicy) {
            		archivingErrorOccured  = true;
            	}
                logger.error("Error when processing policy : " + ard.getWbapName(), e);
                wbarchive.rollbackWork();
                wbarchive.errorArchiveTransaction(e);
            } finally {
                transactions.add(wbarchive.getTransactionData());
                context.getConnectionCore().commit();
            }
        }
    }

    private void processArchiveSchemaPurge(WBPurge wbpurge) throws WBArchiveException , SQLException{
        wbpurge.processArchiveSchemaPurge();
    }


    /**
     * updateBoundaryDate updates the /system/archiving/ARCHIVE_BOUNDARY_DATE
     * registry entry. if isGlobal instance variable is true, then it updates the 
     * global registry and any necessary overrides. if isGlobal is false
     * the client specific overrides are updated or created.
     * @throws WBArchiveException throw if an error occurs while trying to update
     * 				the ARCHIVE_BOUNDARY_DATE parameter.
     */
    public void updateBoundaryDate() throws WBArchiveException {
    	
    	if (archiveEnabledPolicyExists) {
        	if (!archivingErrorOccured) {
            	String date = DateHelper.convertDateString(boundaryDate, "MM/dd/yyyy");
            	RegistryHelper rh = new RegistryHelper();
            	try {
           			rh.setVar(ARCHIVE_BOUNDARY_DATE, date);
            	}
            	catch (NamingException ne) {
            		logger.error("Could not update registry parameter: " + ARCHIVE_BOUNDARY_DATE +
            				" with value: " +  date, ne);
            		throw new WBArchiveException(ne);
            	}
        	}
        	else {
        		logger.error(ARCHIVE_BOUNDARY_DATE + " could not be updated due to an error occuring in the archiving task. " +
        				"Archiving data may be in an inconsistent state.");
        	}
        }   	
    }

    /**
     * Indicates if archiving error(s) occurred. 
     * Returns true if any policy was not successfully run.  
     */
    public boolean hasArchivingErrors() {
    	if (archiveEnabledPolicyExists && archivingErrorOccured) {
    		return true;
    	}
    	return false;
    }

    /**
     * Returns the transactions created by <code>process</code> and hence
     * must be called after <code>process</code>
     * @return list of <code>WBArchTransData</code>
     */
    public List<WBArchTransData> getTransactionsCreated() {
        return transactions;
    }
    
	public String getAgingDate() {
		return this.agingDate;
	}
	
	public void setAgingDate(String agingDate) {
		
		// TODO Auto-generated method stub
		this.agingDate = agingDate;
		
	}
	
	public String getAgingSelection() {
		return this.agingSelection;
	}
	
	public void setAgingSelection(String agingSelection) {
		
		// TODO Auto-generated method stub
		this.agingSelection = agingSelection;
		
	}

    protected void log( String message ) {
        if (logger.isEnabledFor(Level.DEBUG)) { logger.debug( message );}
    }


    protected long meterTime(String what, long start){
        long l = System.currentTimeMillis();
        log("\t"+what+" took: "+(l-start)+" millis");
        return l;
    }

 }

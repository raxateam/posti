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

package com.itella.app.wbarchive.model;

import com.workbrain.app.wbarchive.SchemaHelper;
import com.workbrain.app.wbarchive.WBArchiveContext;
import com.workbrain.app.wbarchive.WBArchiveException;
import com.workbrain.app.wbarchive.db.WBArchPolicyAccess;
import com.workbrain.app.wbarchive.model.WBArchPolDetData;
import com.workbrain.app.wbarchive.model.WBArchPolParamData;
import com.workbrain.app.wbarchive.model.WBArchPolicyData;
import com.workbrain.sql.DBConnection;
import com.workbrain.sql.DBServer;
import com.workbrain.sql.SQLUtil;
import com.workbrain.util.DateHelper;
import com.workbrain.util.LongList;
import com.workbrain.util.StringUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Model class for WBArchive which manages all policy related data.
 */
public class PurgeData {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(PurgeData.class);

    private WBArchPolicyData archivePolicyData;
    private DBConnection connCore;
    private DBConnection connArch;
    private List<WBArchPolDetData> archivePolDetailsOriginal;
    private List<WBArchPolDetData> archivePolDetails;
    private List<WBArchPolParamData> archivePolicyParams;
    private Date archiveRunDate;
    private Date archiveAgingDate;

    public PurgeData(WBArchiveContext context,
                     WBArchPolicyData data) {
        this.connCore = context.getConnectionCore();
        this.connArch = context.getConnectionArchive();
        this.archivePolicyData = data;
        this.archiveRunDate = DateHelper.getCurrentDate();
        this.archiveAgingDate = DateHelper.addDays(archiveRunDate , -1 * data.getWbapAgingDays());
        // *** SQL Server, DB2 do not accept very early dates
        if (this.archiveAgingDate.before(DateHelper.DATE_1900)) {
            this.archiveAgingDate = DateHelper.DATE_1900;
        }
    }

    /**
     * loadPolicy loads the policy and its details
     * @throws WBArchiveException if the policy and its details could not be loaded
     */
    public void loadPolicy() throws WBArchiveException {
    	this.archivePolicyParams = WBArchPolicyAccess.loadArchivePolicyParam(connCore,
                archivePolicyData.getWbapId());
        this.archivePolDetailsOriginal = WBArchPolicyAccess.loadArchivePolicyDetail(connCore,
                archivePolicyData.getWbapId());
        this.archivePolDetails = resolveOriginalPolicyDetails(archivePolDetailsOriginal ,
            archiveAgingDate);
    }
    
    /**
     * Returns the aging date for loaded archive policy.
     *
     * @return
     */
    public Date getArchiveAgingDate() {
        return archiveAgingDate;
    }

    /**
     * Returns policy running date.
     *
     * @return
     */
    public Date getArchiveRunDate() {
        return archiveRunDate;
    }

    /**
     * Returns unresolved policy details for loaded policy.
     *
     * @return
     */
    public List<WBArchPolDetData> getArchivePolicyDetailsOriginal() {
        return archivePolDetailsOriginal;
    }

    /**
     * Returns resolved policy details for loaded policy.
     *
     * @return
     */
    public List<WBArchPolDetData> getArchivePolicyDetails() {
        return archivePolDetails;
    }

    /**
     * Returns ArchivePolicyDayTokenizer for EVERY_X_DAYS archive policies
     * based on commit params.
     *
     * @return ArchivePolicyDayTokenizer
     */
    public ArchivePolicyDayTokenizer getArchivePolicyDayTokenizer(){
        Date startDate = archivePolicyData.retrieveCommitParamValueAsDate(
            WBArchPolicyData.COMMIT_PARAM_START_DATE , "MM/dd/yyyy");
        int days = archivePolicyData.retrieveCommitParamValueInt(WBArchPolicyData.COMMIT_PARAM_BATCH_DAYS);
        return new ArchivePolicyDayTokenizer(startDate , days , archiveAgingDate);
    }

    /**
     * Returns ArchivePolicyEmpIdTokenizer for EVERY_X_EMPLOYEE archive policies
     * based on commit params.
     *
     * @return ArchivePolicyDayTokenizer
     */
    public ArchivePolicyEmpIdTokenizer getArchivePolicyEmpIdTokenizer() throws SQLException {
        int btchSize = archivePolicyData.retrieveCommitParamValueInt(
            WBArchPolicyData.COMMIT_PARAM_EMP_BATCH_SIZE);
        int priority = archivePolicyData.retrieveCommitParamValueInt(WBArchPolicyData.COMMIT_PARAM_CONTROLLING_POLICY_PRIORITY);
        return new ArchivePolicyEmpIdTokenizer(btchSize , priority , connArch);
    }


    /**
     * Returns ArchivePolicyRecordTokenizer for EVERY_X_RECORDS archive policies
     * based on commit params.
     *
     * @return ArchivePolicyRecordTokenizer
     */
    public ArchivePolicyRecordTokenizer getArchivePolicyRecordTokenizer(WBArchPolDetData det) throws SQLException {
        int btchRecCnt = archivePolicyData.retrieveCommitParamValueInt(
            WBArchPolicyData.COMMIT_PARAM_BATCH_RECORD_COUNT);
        return new ArchivePolicyRecordTokenizer(det , btchRecCnt , connArch);
    }

    /**
     * Returns the policy parameter data for given parameter name.
     *
     * @param paramName
     * @return
     */
    public WBArchPolParamData getArchivePolicyParam(String paramName) {
        WBArchPolParamData ret = null;

        Iterator<WBArchPolParamData> iter = archivePolicyParams.iterator();
        while (iter.hasNext()) {
            WBArchPolParamData item = iter.next();
            if (item.getWbappName().equalsIgnoreCase(paramName)) {
                ret = item;
            }
        }

        return ret;
    }

    /**
     * Returns the policy parameter value for given parameter name to be used in sql.
     *
     * @param paramName
     * @return
     */
    public String getArchivePolicyParamResolvedForSql(String paramName) throws SQLException{
        WBArchPolParamData param = getArchivePolicyParam(paramName);
        if (param != null) {
            return param.retrieveParamValueResolvedForSql(connCore);
        }
        return null;
    }

    /**
     * Returns the resolved policy detail for given table name
     *
     * @param tableName
     * @return
     */
    public WBArchPolDetData getArchivePolicyDetail(String tableName) {
        return getArchivePolicyDetail(getArchivePolicyDetails() , tableName , -1) ;
    }

    /**
     * Returns the resolved policy detail for given table name and priority.
     *
     * @param tableName
     * @param priority    if -1. will be ignored in comparison
     * @return
     */
    public WBArchPolDetData getArchivePolicyDetail(String tableName, int priority) {
        return getArchivePolicyDetail(getArchivePolicyDetails() , tableName , priority) ;
    }

    /**
     * Returns the resolved policy detail for given priority.
     *
     * @param tableName
     * @param priority    if -1. will be ignored in comparison
     * @return
     */
    public WBArchPolDetData getArchivePolicyDetail(int priority) {
        return getArchivePolicyDetail(getArchivePolicyDetails() , null , priority) ;
    }

    /**
     * Returns the unresolved policy detail for given table name.
     *
     * @param tableName
     * @param priority    if -1. will be ignored in comparison
     * @return
     */
    public WBArchPolDetData getArchivePolicyDetailOriginal(String tableName) {
        return getArchivePolicyDetailOriginal(tableName , -1);
    }

    /**
     * Returns the unresolved policy detail for given table name and priority.
     *
     * @param tableName
     * @param priority    if -1. will be ignored in comparison
     * @return
     */
    public WBArchPolDetData getArchivePolicyDetailOriginal(String tableName, int priority) {
        return getArchivePolicyDetail(getArchivePolicyDetailsOriginal() , tableName , priority) ;
    }

    /**
     * Returns the unresolved policy detail for given priority.
     *
     * @param tableName
     * @param priority    if -1. will be ignored in comparison
     * @return
     */
    public WBArchPolDetData getArchivePolicyDetailOriginal(int priority) {
        return getArchivePolicyDetail(getArchivePolicyDetailsOriginal() , null , priority) ;
    }

    private WBArchPolDetData getArchivePolicyDetail(List<WBArchPolDetData> details ,
        String tableName, int priority) {
        if (details == null || details.size() == 0) {
            return null;
        }
        WBArchPolDetData ret = null;

        Iterator<WBArchPolDetData> iter = details.iterator();
        while (iter.hasNext()) {
            WBArchPolDetData item = iter.next();
            if (StringUtil.isEmpty(tableName)
                || item.getWbapdTableName().equalsIgnoreCase(tableName)) {
                if (priority == -1
                    || (priority != -1 && priority == item.getWbapdPriority())) {
                    ret = item;
                    break;
                }
            }
        }

        return ret;
    }

    /**
     * Resolves original policy details for given aging date.
     *
     * @param arcDetails
     * @param agingDate
     * @return
     * @throws WBArchiveException
     */
    public List<WBArchPolDetData> resolveOriginalPolicyDetails(List<WBArchPolDetData> arcDetails , Date agingDate)
            throws WBArchiveException{
        List<WBArchPolDetData> resolved = new ArrayList<WBArchPolDetData>();

        for (int i=0; i < arcDetails.size() ; i++) {
            WBArchPolDetData item = arcDetails.get(i);
            String resolvedWhere = resolvePolicyDetailForWHERE(item.getWbapdWhereClause() , resolved);
            resolvedWhere = resolvePolicyDetailForPARAM(resolvedWhere);
            resolvedWhere = resolvePolicyDetailWhereForAGINGDATE(resolvedWhere ,
                agingDate);
            resolvedWhere = resolvePolicyDetailWhereForCURRENTDATETIME(resolvedWhere);

            log(item.getWbapdWhereClause() + " <___RESOLVED_TO___> " + resolvedWhere);
            item.assignWbapdWhereClauseResolved(resolvedWhere);
            resolved.add(item);
        }
        return resolved;
    }

    /**
     * Resolves already resolved policy details for given empIds.
     *
     * @param arcDetails
     * @param agingDate
     * @return
     * @throws WBArchiveException
     */
    public List<WBArchPolDetData> resolvePolicyDetails(List<WBArchPolDetData> arcDetails , List<String> empIds)
            throws WBArchiveException{
        List<WBArchPolDetData> resolved = new ArrayList<WBArchPolDetData>();

        for (int i=0; i < arcDetails.size() ; i++) {
            WBArchPolDetData item = arcDetails.get(i);
            String where = item.retrieveWbapdWhereClauseResolved();
            if (where.indexOf(WBArchPolDetData.WHERE_EMP_ID_SELECTOR) < 0) {
                resolved.add(item);
                continue;
            }

            String resolvedWhere = resolvePolicyDetailWhereForEMPIDSELECTOR(where , empIds);

            log(item.getWbapdWhereClause() + " <___RESOLVED_TO___> " + resolvedWhere);
            WBArchPolDetData newItem = item.duplicate();
            newItem.assignWbapdWhereClauseResolved(resolvedWhere);
            resolved.add(newItem);
        }
        return resolved;
    }

    /**
     * Resolves given where clause.
     *
     * @param whereClause
     * @param alreadyResolved  list of WBArchPolDetData objects which might have been already resolved
     *                         and should be looked at for referring WHERES. If null ignore
     * @return
     * @throws WBArchiveException
     */
    public String resolvePolicyDetailForWHERE(String whereClause ,
                                              List<WBArchPolDetData> alreadyResolved) throws WBArchiveException{
        String finWhereClause = whereClause;
        try {
            /*  parsing #WHERE.tableName.[priority]# */
            int indWhere = finWhereClause.indexOf(WBArchPolDetData.WHERE_WHERE_PREFIX);
            while (indWhere > -1) {
                int indEnd = finWhereClause.indexOf(WBArchPolDetData.WHERE_MARKER, indWhere + 1);
                String tabName = finWhereClause.substring(
                    indWhere + WBArchPolDetData.WHERE_WHERE_PREFIX.length(),
                    indEnd);
                int indPriority = tabName.indexOf(".");
                int priority = -1;
                if (indPriority > -1) {
                    tabName = tabName.substring(0, indPriority);
                    priority = Integer.parseInt(tabName.substring(indPriority + 1));
                }
                /* first check already resolved */
                WBArchPolDetData whereClauseThisTable = getArchivePolicyDetail(
                    alreadyResolved , tabName , priority);
                /* if not in already resolved, look at the original */
                if (whereClauseThisTable == null) {
                    whereClauseThisTable = getArchivePolicyDetailOriginal(tabName, priority);
                }
                if (whereClauseThisTable != null) {
                    finWhereClause = StringUtil.searchReplace(finWhereClause,
                        WBArchPolDetData.WHERE_WHERE_PREFIX + tabName + WBArchPolDetData.WHERE_MARKER,
                        whereClauseThisTable.getWbapdWhereClause());
                } else {
                    throw new WBArchiveException("Table : " + tabName
                        + " is referred in an archive detail"
                        + " with #WHERE. syntax but does not exist in any archive detail definition");
                }
                indWhere = finWhereClause.indexOf(WBArchPolDetData.WHERE_WHERE_PREFIX);
            }
        } catch (Exception e) {
        	logger.error("error occurred trying to resolve policy detail for where clause", e);
            throw new WBArchiveException ("Error in parsing Where clause for : " + whereClause, e);
        }
        return finWhereClause;
    }

    /**
     * Resolves given where clause.
     *
     * @param whereClause
     * @return
     * @throws WBArchiveException
     */
    public String resolvePolicyDetailForPARAM(String whereClause) throws WBArchiveException{
        String finWhereClause = whereClause;
        try {
            /*  parsing #PARAM.tableName.[priority]# */
            int indWhere = finWhereClause.indexOf(WBArchPolDetData.WHERE_PARAM_PREFIX);
            while (indWhere > -1) {
                int indEnd = finWhereClause.indexOf(WBArchPolDetData.WHERE_MARKER, indWhere + 1);
                String paramName = finWhereClause.substring(
                    indWhere + WBArchPolDetData.WHERE_PARAM_PREFIX.length(),
                    indEnd);
                String paramVal = getArchivePolicyParamResolvedForSql(paramName);
                if (paramVal != null) {
                    finWhereClause = StringUtil.searchReplace(finWhereClause,
                        WBArchPolDetData.WHERE_PARAM_PREFIX + paramName + WBArchPolDetData.WHERE_MARKER,
                        paramVal);
                }
                else {
                    throw new WBArchiveException("Parameter : " + paramName
                        + " is not defined for the archive policy");
                }
                indWhere = finWhereClause.indexOf(WBArchPolDetData.WHERE_WHERE_PREFIX);
            }
        } catch (Exception e) {
            logger.error("Error in parsing Where clause" , e);
            throw new WBArchiveException ("Error in parsing Where clause for : " + whereClause);
        }
        return finWhereClause;
    }

    /**
     * Resolves given statement for given aging date.
     *
     * @param whereClause
     * @param agingDate
     * @return
     * @throws WBArchiveException
     */
    public String resolvePolicyDetailWhereForAGINGDATE(String whereClause , Date agingDate)
            throws WBArchiveException{
        return StringUtil.searchReplace(whereClause,
                WBArchPolDetData.WHERE_AGING_DAYS ,
                connCore.encodeTimestamp(agingDate));
    }

    /**
     * Resolves given statement for cuurent datetime constants.
     *
     * @param whereClause
     * @return
     * @throws WBArchiveException
     */
    public String resolvePolicyDetailWhereForCURRENTDATETIME(String whereClause)
            throws WBArchiveException{
        return StringUtil.searchReplace(whereClause,
                WBArchPolDetData.WHERE_CURRENT_DATETIME ,
                connCore.encodeCurrentTimestamp());
    }

    /**
     * Resolves given statement for given employee ids.
     *
     * @param whereClause
     * @param agingDate
     * @return
     * @throws WBArchiveException
     */
    public String resolvePolicyDetailWhereForEMPIDSELECTOR(String whereClause , List<String> empIds)
            throws WBArchiveException{

        String empIdCSV = StringUtil.createCSVForCharacter(empIds);
        empIdCSV = StringUtil.searchReplace(empIdCSV , "'" ,"");

        String repString = StringUtil.isEmpty(empIdCSV) ? "" : " emp_id in (" + empIdCSV + ")";
        return StringUtil.searchReplace(whereClause ,
            WBArchPolDetData.WHERE_EMP_ID_SELECTOR , repString);

    }


    /**
     * Tokenizer that will tokenize archive detail list for each
     * given batch days
     *
     */
    public class ArchivePolicyDayTokenizer {
        private List<WBArchPolDetData> details;
        private Date startDate;
        private Date endDate;
        private Date loopDate;
        private int batchDays;

        public ArchivePolicyDayTokenizer(Date startDate, int batchDays , Date endDate) {
            this.details = getArchivePolicyDetailsOriginal();
            this.startDate = startDate;
            this.endDate = endDate;
            this.batchDays = batchDays;
            this.loopDate = new Date(DateHelper.truncateToDays(startDate).getTime());
        }

        public boolean hasNext() {
            return this.loopDate.compareTo(endDate) < 0;
        }

        public List<WBArchPolDetData> next() throws WBArchiveException {
            this.loopDate = DateHelper.addDays(loopDate, this.batchDays);
            if (this.loopDate.compareTo(endDate) > 0) {
                this.loopDate = endDate;
            }
            List<WBArchPolDetData> thisBatchDetails = resolveOriginalPolicyDetails(this.details, this.loopDate);
            return thisBatchDetails;
        }

        public Date getCurrentAgingDate() {
            return this.loopDate;
        }

    }

    /**
     * Tokenizer that will tokenize archive detail list for each
     * given employee batch size.
     *
     */
    public class ArchivePolicyEmpIdTokenizer {
        private List<WBArchPolDetData> details;
        private int empBatchSize;
        private List<String> empIds = new ArrayList<String>();
        private int currentInd = 0;
        private int allEmpCount = 0;
        private int currentBatchSize = 0;
        private List<String> thisBatchEmpIds = null;

        public ArchivePolicyEmpIdTokenizer(int empBatchSize ,
                                           int priorityNumber ,
                                           DBConnection conn) throws SQLException{
            log ("ArchivePolicyEmpIdTokenizer empBatchSize = " + empBatchSize + " priorityNumber = " + priorityNumber);

            WBArchPolDetData det = getArchivePolicyDetail( priorityNumber);
            Statement st = null;
            ResultSet rs = null;
            try {
                String sql = det.retrieveWbapdWhereClauseResolved();
                if (sql.indexOf(WBArchPolDetData.WHERE_EMP_ID_SELECTOR) < 0) {
                    throw new RuntimeException ("Controlling sql does not have "
                                                + WBArchPolDetData.WHERE_EMP_ID_SELECTOR + " keyword");
                }
                sql = StringUtil.searchReplace(sql , WBArchPolDetData.WHERE_EMP_ID_SELECTOR , " 1=1 ");
                String finalSql = "SELECT distinct (emp_id) FROM " + det.getWbapdTableName()
                    + " WHERE " + sql + " ORDER BY emp_id";
                log ("Controlling sql = " + finalSql);
                st = conn.createStatement(); //NOPMD
                rs = st.executeQuery(finalSql);
                while (rs.next()) {
                    String empId = rs.getString(1);
                    empIds.add(empId);
                }
                allEmpCount = empIds.size();
                log("allEmpCount = " + allEmpCount);
            } finally {
                if (rs != null) rs.close();
                if (st != null) st.close();
            }

            this.details = getArchivePolicyDetails();
            this.empBatchSize = empBatchSize;
        }

        public boolean hasNext() {
            return currentInd  < allEmpCount;
        }

        public List<WBArchPolDetData> next() throws WBArchiveException {
            int endInd = Math.min(allEmpCount , currentInd + empBatchSize ) ;
            currentBatchSize = endInd - currentInd;
            thisBatchEmpIds = new ArrayList<String>(empIds.subList(currentInd , endInd));
            this.currentInd += currentBatchSize;
            List<WBArchPolDetData> thisBatchDetails = resolvePolicyDetails(this.details, thisBatchEmpIds);
            return thisBatchDetails;
        }

        public int getCurrentCount() {
            return this.currentInd;
        }

        /**
         * List of current EmpIds.
         * @return
         */
        public LongList getCurrentEmpIds() {
        	LongList empIds = new LongList();
            empIds.addAllString(thisBatchEmpIds);
            return empIds;
        }

        public int getCurrentBatchSize() {
            return this.currentBatchSize;
        }
    }

    /**
     * Tokenizer that will produce sqls for given batch size.
     *
     */
    public class ArchivePolicyRecordTokenizer {
        private int currentCount = 0;
        private int allRecordCount = 0;
        private int batchRecordCount = 0;
        private WBArchPolDetData det;
        private DBConnection conn;
        private String pkCol;

        public ArchivePolicyRecordTokenizer(WBArchPolDetData det,
                                            int batchRecordCount ,
                                            DBConnection conn) throws SQLException{
            log ("ArchivePolicyRecordTokenizer recordCount = " + batchRecordCount);
            this.batchRecordCount = batchRecordCount;
            this.det = det;
            this.conn = conn;

            String sql = det.retrieveWbapdWhereClauseResolved();
           	allRecordCount = getNumberOfRecords(sql);
            log("allRecordCount = " + allRecordCount);
            // *** find pkcol for DB2 and MSSQL
            if (conn.getDBServer().getId() == DBServer.DB2_ID
               || conn.getDBServer().getId() == DBServer.MSSQL_ID) {
                pkCol = SchemaHelper.getPrimaryKeyColumnForTable(conn ,
                    det.getWbapdTableName());
            }
        }

        public boolean hasNext() {
            return currentCount  < allRecordCount;
        }

        /**
         * Return where clauses for row limit.
         *
         * @return
         * @throws WBArchiveException
         */
        public String next() throws WBArchiveException, SQLException {
            StringBuilder sb = new StringBuilder(400);
            if (conn.getDBServer().getId() == DBServer.ORACLE_ID ) {
                sb.append(det.retrieveWbapdWhereClauseResolved());
                sb.append(" AND ").append(conn.encodeRowLimit(batchRecordCount));
            }
            else if (conn.getDBServer().isDB2_OS390()) {
            	sb.append(det.retrieveWbapdWhereClauseResolved());
            	sb.append(" AND ");
            	sb.append(pkCol).append(" >= ( SELECT min (");
            	sb.append(pkCol).append(")");
            	sb.append(" FROM ").append(det.getWbapdTableName());
                sb.append(" WHERE ").append(det.retrieveWbapdWhereClauseResolved());
                sb.append(") AND ");

                sb.append(pkCol).append(" < ( SELECT min (");
                sb.append(pkCol).append(") + ").append(batchRecordCount);
                sb.append(" FROM ").append(det.getWbapdTableName());
                sb.append(" WHERE ").append(det.retrieveWbapdWhereClauseResolved());
                sb.append(")");
            }
            else if (conn.getDBServer().getId() == DBServer.DB2_ID ) {
                sb.append(pkCol).append(" IN (SELECT ");
                sb.append(pkCol);
                sb.append(" FROM ").append(det.getWbapdTableName());
                sb.append(" WHERE ").append(det.retrieveWbapdWhereClauseResolved());
                if(!(conn.getDBServer().isDB2_OS390())) {
                  sb.append(conn.encodeRowLimit(batchRecordCount));
                }
                sb.append(")");
            }
            else if (conn.getDBServer().getId() == DBServer.MSSQL_ID ) {
            	sb.append(pkCol).append(" IN (SELECT ");
                sb.append(conn.encodeRowLimit(batchRecordCount)).append(pkCol);
                sb.append(" FROM ").append(det.getWbapdTableName());
                sb.append(" WHERE ").append(det.retrieveWbapdWhereClauseResolved());
                sb.append(")");
            }

            // determine the number of processed records
            if (!conn.getDBServer().isDB2_OS390()) {
            	currentCount += batchRecordCount;
            } else {
            	// zOS only: The number of records returned by the where clause could be less than the batch record count
            	currentCount += getNumberOfRecords(sb.toString());
            }
            return sb.toString();
        }

        public boolean hasAny() {
            return allRecordCount > 0;
        }

        private int getNumberOfRecords(String where) throws SQLException {
            PreparedStatement ps = null;
            ResultSet rs = null;

        	try {
				String finalSql = "SELECT count(*) FROM " + det.getWbapdTableName()
				+ " WHERE " + where;
				log ("Count sql = " + finalSql);
				ps = conn.prepareStatement(finalSql);
				rs = ps.executeQuery();
				if  (rs.next()) {
					return rs.getInt(1);
				}
			} finally {
				SQLUtil.cleanUp(ps,rs);
			}
        	return 0;
        }

    }

    protected long meterTime(String what, long start){
        long l = System.currentTimeMillis();
        if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) { logger.debug("\t"+what+" took: "+(l-start)+" millis");}
        return l;
    }

    public void log( String message ) {
        if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) { logger.debug( message );}
    }
}

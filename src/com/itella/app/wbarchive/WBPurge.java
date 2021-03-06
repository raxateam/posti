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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import com.workbrain.app.wbarchive.SchemaHelper;
import com.workbrain.app.wbarchive.WBArchiveContext;
import com.workbrain.app.wbarchive.WBArchiveException;
import com.workbrain.app.wbarchive.db.WBArchTransAccess;
import com.workbrain.app.wbarchive.model.ArchiveData;
import com.itella.app.wbarchive.model.PurgeData;
import com.workbrain.app.wbarchive.model.WBArchPolDetData;
import com.workbrain.app.wbarchive.model.WBArchPolicyData;
import com.workbrain.app.wbarchive.model.WBArchTransData;
import com.workbrain.app.wbarchive.model.WBArchTransDetData;
import com.workbrain.sql.DBConnection;
import com.workbrain.sql.SQLUtil;
import com.workbrain.tool.mail.Message;
import com.workbrain.util.DataLoader;
import com.workbrain.util.FileUtil;
import com.workbrain.util.JavaUtil;
import com.workbrain.util.LongArrayIterator;
import com.workbrain.util.LongList;
import com.workbrain.util.StringUtil;
import com.workbrain.util.WBFile;
import com.workbrain.util.XMLHelper;

//Main Archive Process Task
import com.workbrain.app.wbarchive.WBArchiveProcessTask;

/**
 * API class for creating Archiving implementations.
 */
public class WBPurge {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(WBPurge.class);


    private static final String TEMP_FILE_PREFIX = "wbarchiveTemp";
    private static final String TEMP_FILE_CFG_EXT = ".cfg";
    private static final String TEMP_FILE_XML_EXT = ".xml";

    private static final String HTML_BREAK = "<br>";
    private static final int SQL_IN_CLAUSE_LIMIT = 500;
    private static final long WB_USER_ID = 3;
    private static final String WB_USER_NAME = "WORKBRAIN";
    private WBArchPolicyData archivePolicyData;
    private DBConnection connCore;
    private DBConnection connArch;
    private WBArchTransData transData = null;
    private StringBuffer transMsg = new StringBuffer (400);
    private int totalRecordsAffected = 0;
    private TransactionDetails transactionDetails;
    private String tempFilePath;
    private String cfgFilePath;
    private String xmlFilePath;
    private boolean isArchiveModeDB;
    private boolean isArchiveModeSchema;
    private ArchiveData archiveData;
    private PurgeData purgeData;
    private String schemaUserNameArchive;
    private WBArchiveProcessTask archiveTask;

    private final MessageFormat configXmlFormat = new MessageFormat(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
                    + "<config name=\"workbrain\" version=\"0.9.3\" export=\"{0}\">"
                    + "    <table name=\"{0}\" where=\"{1}\"/>\n"
                    + "</config>");

    public WBPurge(WBArchiveContext context,
                     WBArchPolicyData data) {
        this(context, data, null);
    }

    public WBPurge(WBArchiveContext context,
                     WBArchPolicyData data,
                     WBArchiveProcessTask archiveTask) {
        this.connCore = context.getConnectionCore();
        this.connArch = context.getConnectionArchive();
        this.archivePolicyData = data;
        this.isArchiveModeDB = context.isArchiveModeDB();
        if (isArchiveModeDB) {
            this.tempFilePath = context.getTempFilePath();
            if (!StringUtil.isEmpty(tempFilePath)) {
                if (!tempFilePath.endsWith(File.separator)) {
                    this.tempFilePath += File.pathSeparator;
                }
            }
        }
        this.isArchiveModeSchema = context.isArchiveModeSCHEMA();
        archiveData = new ArchiveData(context, data);
        purgeData = new PurgeData(context, data);
        this.totalRecordsAffected = 0;
        this.schemaUserNameArchive = context.getSchemaUserNameArchive();
        this.archiveTask = archiveTask;
    }

    /**
     * Returns core database connection
     *
     * @return
     */
    public DBConnection getDBConnectionCore() {
        return connCore;
    }

    /**
     * Returns ArchiveData which manages all policy related data.
     *
     * @return
     */
    public ArchiveData getArchiveData() {
        return archiveData;
    }

    /**
     * Returns loaded archive policy.
     *
     * @return
     */
    public WBArchPolicyData getArchivePolicyData() {
        return archivePolicyData;
    }

    /**
     * Returns the aging date for loaded archive policy.
     *
     * @return
     */
    public Date getArchiveAgingDate() {
        return archiveData.getArchiveAgingDate();
    }

    /**
     * Returns policy running date.
     *
     * @return
     */
    public Date getArchiveRunDate() {
        return archiveData.getArchiveRunDate();
    }

    /**
     * Returns unresolved policy details for loaded policy.
     *
     * @return
     */
    public List<WBArchPolDetData> getArchivePolicyDetailsOriginal() {
        return archiveData.getArchivePolicyDetailsOriginal();
    }

    /**
     * Returns resolved policy details for loaded policy.
     *
     * @return
     */
    public List<WBArchPolDetData> getArchivePolicyDetails() {
        return archiveData.getArchivePolicyDetails();
    }

    /**
     * Returns ArchivePolicyDayTokenizer for EVERY_X_DAYS archive policies
     * based on commit params.
     *
     * @return ArchivePolicyDayTokenizer
     */
    public ArchiveData.ArchivePolicyDayTokenizer getArchivePolicyDayTokenizer(){
        return archiveData.getArchivePolicyDayTokenizer();
    }

    /**
     * Returns ArchivePolicyDayTokenizer for EVERY_X_EMPLOYEES archive policies
     * based on commit params.
     *
     * @return ArchivePolicyDayTokenizer
     */
    public ArchiveData.ArchivePolicyEmpIdTokenizer getArchivePolicyEmpIdTokenizer() throws SQLException{
        return archiveData.getArchivePolicyEmpIdTokenizer();
    }

    /**
     * Returns ArchivePolicyDayTokenizer for EVERY_X_DAYS archive policies
     * based on commit params.
     *
     * @return ArchivePolicyRecordTokenizer
     */
    public ArchiveData.ArchivePolicyRecordTokenizer getArchivePolicyRecordTokenizer
    (WBArchPolDetData det) throws SQLException{
        return archiveData.getArchivePolicyRecordTokenizer(det);
    }

    /**
     * Processes archive policy be default settings.
     *
     * @throws WBArchiveException
     * @throws SQLException
     */
    public void processArchivePolicy() throws WBArchiveException, SQLException{
        if (getArchivePolicyData().isCommitTypeEveryPolicyDetail()
                || getArchivePolicyData().isCommitTypeAllOrNothing()) {
            List<WBArchPolDetData> archDat = getArchivePolicyDetails();
            Iterator<WBArchPolDetData> iter = archDat.iterator();
            while (iter.hasNext()) {
                WBArchPolDetData archDet = iter.next();
                processArchivePolicyDetail(archDet,
                        getArchivePolicyData().isCommitTypeEveryPolicyDetail());
            }
            if (getArchivePolicyData().isCommitTypeAllOrNothing()) {
                commitWork();
            }
        }
        else if (getArchivePolicyData().isCommitTypeEveryXDays()) {
            ArchiveData.ArchivePolicyDayTokenizer tok = getArchivePolicyDayTokenizer();
            while (tok.hasNext()) {
                List<WBArchPolDetData> detailsThisBatch = tok.next();
                String log = "Processing days before " + tok.getCurrentAgingDate();
                log(log);
                addTransactionDetailMessage(log);
                processArchivePolicyDetails(detailsThisBatch, true);
            }

        }
        else if  (getArchivePolicyData().isCommitTypeEveryXEmployee()) {
            ArchiveData.ArchivePolicyEmpIdTokenizer tok = getArchivePolicyEmpIdTokenizer();
            while (tok.hasNext()) {
                List<WBArchPolDetData> detailsThisBatch = tok.next();
                String log = "Processing " + tok.getCurrentBatchSize() + " employee(s)";
                log(log);
                addTransactionDetailMessage(log);
                processArchivePolicyDetails(detailsThisBatch, true);
            }
        }
        else if (getArchivePolicyData().isCommitTypeEveryXRecords()) {
            List<WBArchPolDetData> archDat = getArchivePolicyDetails();
            Iterator<WBArchPolDetData> iter = archDat.iterator();
            while (iter.hasNext()) {
                WBArchPolDetData archDet = iter.next();
                ArchiveData.ArchivePolicyRecordTokenizer tok = getArchivePolicyRecordTokenizer(archDet);
                if (!tok.hasAny()) {
                    String log = "No records found for this policy detail";
                    log(log); addTransactionDetailMessage(log);
                }
                else {
                    while (tok.hasNext()) {
                        String sqlThisBatch = tok.next();
                        String log = "Processing BATCH SQL : \n" + sqlThisBatch;
                        log(log);  addTransactionDetailMessage(log);
                        WBArchPolDetData archDetThis = new WBArchPolDetData();
                        archDetThis.setWbapdTableName(archDet.getWbapdTableName());
                        archDetThis.setWbapdWhereClause(sqlThisBatch);
                        archDetThis.assignWbapdWhereClauseResolved(sqlThisBatch);
                        processArchivePolicyDetail(archDetThis, true);
                    }
                }
            }
        }
    }

    /**
     * Processes list of resolved ArchivePolicyDetails.
     *
     * @param policyDetails
     * @param shouldCommit
     */
    public void processArchivePolicyDetails(List<WBArchPolDetData> policyDetails,
                                            boolean shouldCommit)
            throws SQLException , WBArchiveException {
        if (policyDetails == null || policyDetails.size() == 0) {
            return;
        }

        Iterator<WBArchPolDetData> iter = policyDetails.iterator();
        while (iter.hasNext()) {
            WBArchPolDetData archDet = iter.next();
            processArchivePolicyDetail(archDet, false);
        }
        if (shouldCommit) {
            commitWork();
        }

    }

    /**
     * Processes the given resolved archive policy detail.
     *
     * @param archDet
     * @param shouldCommit
     * @throws WBArchiveException
     * @throws SQLException
     */
    public void processArchivePolicyDetail(WBArchPolDetData archDet,
                                           boolean shouldCommit) throws WBArchiveException , SQLException{
        log("Processing archive detail for table " + archDet.getWbapdTableName());

        try {
            // Update Task Lock
            if (archiveTask != null){
                archiveTask.isInterrupted();
            }

            addTransactionDetailItem(archDet);
            if (shouldCommit) {
                commitWork();
            }
        } catch(Exception e) {
            if (shouldCommit) {
                rollbackWork();
            }
            throw new WBArchiveException("Error in processing archive detail : "
                    + archDet.getWbapdTableName()  , e);
        }
    }

    /**
     * Adds detail data to the detail list which will be used in committing the work
     *
     * @param detData
     */
    public void addTransactionDetailItem(WBArchPolDetData detData) throws SQLException {
        if (!detData.resolvesForPrimaryKey()) {
            getTransactionDetails().addToTransactionDetails(detData.
                            getWbapdTableName(),
                    detData.retrieveWbapdWhereClauseResolved());
        }
        else {
            getTransactionDetails().
                    addToTransactionDetailsResolvingToPrimaryKey(detData.getWbapdTableName(),
                            detData.retrieveWbapdWhereClauseResolved()
                    );
        }
    }

    /**
     * Adds msg to to the transaction detail for logging.
     *
     * @param msg
     */
    public void addTransactionDetailMessage(String msg) {
        getTransactionDetails().addToMessage(msg);
    }

    /**
     * Goes through all details created by addTransactionDetail and performs
     * necessary purge and move.
     *
     * @throws WBArchiveException
     * @throws SQLException
     */
    public void commitWork() throws WBArchiveException , SQLException{
        List<TransactionDetails.TransactionDetailItem> transactionDetailItems = getTransactionDetails().getTransactionDetails();
        log("Processing " + (transactionDetailItems == null ? 0 :transactionDetailItems.size())
                + " transaction detail items");
        try {
            if (transactionDetailItems != null && transactionDetailItems.size() != 0) {
                Date start = new Date();
                for (int i = 0, k = transactionDetailItems.size(); i < k; i++) {
                    TransactionDetails.TransactionDetailItem item =
                            transactionDetailItems.get(i);
                    int affectedRecs = 0;
                    if (archivePolicyData.isArchive()) {
                        int cnt = archiveData(item);
                        if (cnt > 0) {
                            affectedRecs = purgeData(item);
                        }
                    }
                    else {
                        affectedRecs = purgeData(item);
                    }
                    String msg = affectedRecs + " records have been processed successfully";
                    log(msg);  addTransactionDetailMessage(msg);
                    addTransactionRecordsAffected(affectedRecs);
                }
                addTransactionDetailMessage("Work Committed");

                deleteTempFiles(true);
                createArchiveTransactionDetails(connCore,
                        transactionDetailItems,
                        start,
                        getTransactionDetails().getMessage());
                if (isArchiveModeDB && archivePolicyData.isArchive()) {
                    connArch.commit();
                }
            }
            connCore.commit();
            log("Work Committed\n\n");
        } catch (Exception e){
            connCore.rollback();
            if (isArchiveModeDB && archivePolicyData.isArchive()) {
                connArch.rollback();
            }
            deleteTempFiles(true);
            resetTransactionRecordsAffected();
            logger.error("Error in committing work", e);
            throw new WBArchiveException(e);
        } finally {
            getTransactionDetails().clearAll();
        }
    }

    /**
     * Creates one WBArchTransDetData based on transactionDetailItems.
     *
     * @param connCore
     * @param transactionDetailItems
     * @param start
     * @throws SQLException
     */
    protected void createArchiveTransactionDetails(DBConnection connCore ,
                                                   List<TransactionDetails.TransactionDetailItem> transactionDetailItems , Date start , String transactionDetailMsg)
            throws SQLException{
        if (transactionDetailItems == null || transactionDetailItems.size() == 0) {
            return;
        }
        WBArchTransDetData transDet = new WBArchTransDetData();
        Iterator<TransactionDetails.TransactionDetailItem> iter = transactionDetailItems.iterator();
        StringBuilder sql = new StringBuilder(1000);
        StringBuilder tableNames = new StringBuilder(200);
        while (iter.hasNext()) {
            TransactionDetails.TransactionDetailItem item =
                    iter.next();
            if (!StringUtil.isEmpty(item.getWhere())) {
                sql.append(item.getWhere()).append(HTML_BREAK);
            }
            if (!StringUtil.isEmpty(item.getTableName())) {
                tableNames.append(item.getTableName()).append(HTML_BREAK);
            }
        }
        transDet.setWbatId(transData.getWbatId());
        transDet.setWbatdTableNames(tableNames.toString());
        transDet.setWbatdMsg(transactionDetailMsg);
        transDet.setWbatdWheres(sql.toString());
        transDet.setWbatdStart(start);
        transDet.setWbatdEnd(new Date());
        transDet.setWbatdStatus(WBArchTransAccess.WBARTD_STATUS_APPLIED);
        WBArchTransAccess.createArchiveTransactionDetail(connCore , transDet);
    }

    /**
     * Rollbacks work that have not been commitWorked.
     *
     * @throws SQLException
     */
    public void rollbackWork() throws SQLException {
        getTransactionDetails().clearAll();
        connCore.rollback();
        if (isArchiveModeDB && archivePolicyData.isArchive()) {
            connArch.rollback();
        }
        deleteTempFiles(false);
    }

    protected TransactionDetails  getTransactionDetails() {
        if (transactionDetails == null) {
            transactionDetails = new TransactionDetails();
        }
        return transactionDetails;
    }

    /**
     * Archives data from given TransactionDetailItem.
     *
     * @param item TransactionDetailItem
     * @return
     * @throws WBArchiveException
     */
    private int archiveData(TransactionDetails.TransactionDetailItem item)
            throws WBArchiveException{
        if (!item.resolvesForPrimaryKey) {
            return archiveData(item.getTableName(), item.getWhere());
        }
        else {
            int ret = 0;
            if (item.getPrimaryKeyIds() != null
                    && item.getPrimaryKeyIds().size() > 0) {
                log("Processing " + item.getPrimaryKeyIds().size() +  " primary key items for " + item.getTableName());
                // *** if it will be archived for PKids, chunk it since IN clause cannot take more than SQL_IN_CLAUSE_LIMIT
                LongArrayIterator iter = new LongArrayIterator(item.
                        getPrimaryKeyIds().toLongArray(), SQL_IN_CLAUSE_LIMIT);
                while (iter.hasNext()) {
                    long[] ids = iter.next();
                    if (ids == null || ids.length == 0)
                        continue;
                    String where = item.getPrimaryKeyColumnName() + " IN ("
                            + StringUtil.createCSVForNumber(new LongList(ids)) +
                            ")";
                    ret += archiveData(item.getTableName(), where);
                }
            }
            return ret;
        }
    }

    /**
     * Archives data from given table where clause to archive datasource.
     *
     * @param tableName
     * @param whereClause
     * @return
     * @throws WBArchiveException
     */
    private int archiveData(String tableName, String whereClause) throws WBArchiveException{
        int cnt = 0;
        try {
            long s = System.currentTimeMillis();
            if (isArchiveModeDB) {
                DataLoader dl = new DataLoader();
                createConfigFile(tableName, whereClause, tempFilePath);
                dl.exportXml(connCore, cfgFilePath, tempFilePath, false);
                dl.importXml(connArch, new String[] {xmlFilePath}
                        , false);
            }
            else if (isArchiveModeSchema){
                PreparedStatement stm = null;
                try {
                    StringBuilder sql = new StringBuilder(200);
                    sql.append("INSERT INTO ");
                    sql.append(schemaUserNameArchive).append(".");
                    sql.append(tableName);
                    if (connCore.getDBServer().isDB2_OS390()){
                        sql.append(" ( ");
                        Iterator<String> iter = getTableColumns(connCore,tableName).iterator();
                        StringBuilder columns = new StringBuilder();
                        if(iter.hasNext()){
                            columns.append(iter.next());
                        }
                        while(iter.hasNext()){
                            columns.append(",");
                            columns.append(iter.next());
                        }
                        sql.append(columns);
                        sql.append(" ) ");
                        sql.append(" SELECT ");
                        sql.append(columns);
                        sql.append(" FROM ");
                        sql.append(tableName);
                        sql.append(" WHERE ").append(whereClause);
                        stm = connCore.prepareStatement(sql.toString(),true, true);
                        cnt = stm.executeUpdate();
                    }else{
                        sql.append(" SELECT * FROM ");
                        sql.append(tableName);
                        sql.append(" WHERE ").append(whereClause);
                        stm = connCore.prepareStatement(sql.toString(),false, false);
                        cnt = stm.executeUpdate();
                    }

                } finally {
                    SQLUtil.cleanUp(stm);
                }
            }
            meterTime("Transferring data for : " + tableName , s);
            if (cnt == 0) {
                log("No records found for tableName : " + tableName + " where : " + whereClause);
            }
        } catch (Exception e) {
            throw new WBArchiveException(e);
        }
        return cnt;
    }
    /**
     * This method filters out [TABLE_NAME]_ROWID column that DB2/zOS includes in tables with CLOBS
     * Since _ROWID column is defined as GENERATED ALWAYS, the value cannot be specified when inserting data 
     * ROWID is zOS specific 
     * @param dbc
     * @param tblName
     * @return
     * @throws WBArchiveException
     */
    private Vector<String> getTableColumns(DBConnection dbc, String tblName) throws WBArchiveException {
        Vector<String> colList = new Vector<String>();
        boolean isDB2zOS = dbc.getDBServer().isDB2_OS390();
        if (isDB2zOS) {
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                pstmt = dbc.prepareStatement("SELECT * FROM " + tblName
                        + " WHERE 1 = 2", true, true);
                rs = pstmt.executeQuery();
                ResultSetMetaData rsmd = rs.getMetaData();

                int nCols = rsmd.getColumnCount();
                for (int i = 1; i <= nCols; i++) {
                    String colName = rsmd.getColumnName(i);
                    /*
                     * Don't process the _ROWID column that DB2/zOS includes in
                     * tables with CLOBS.
                     */
                    if (colName.endsWith("_ROWID")) {
                        continue;
                    }
                    // Add a column definition to the table
                    colName = dbc.physicalToLogical(colName);
                    colList.add(colName);
                }
            } catch (SQLException e) {
                log (e.getMessage());
                throw new WBArchiveException(e);
            }finally{
                SQLUtil.cleanUp(pstmt,rs);
            }
        }
        return colList;
    }

    /**
     * Returns records for given table and where clause
     *
     * @param tableName
     * @param whereClause
     * @return
     * @throws SQLException
     */
    public int getCountForTableWhereClause(String tableName, String whereClause)
            throws SQLException {
        Statement stm = null;
        ResultSet rs = null;
        int cnt = 0;
        try {
            stm = connCore.createStatement();
            String sql = "SELECT count(*) FROM "
                    + tableName + " WHERE " + whereClause;
            rs = stm.executeQuery(sql);
            if (rs.next()) {
                cnt = rs.getInt(1);
            }
        } finally {
            if (stm != null) stm.close();
            if (rs != null) rs.close();
        }
        return cnt;
    }

    /**
     * Purges data from given TransactionDetailItem.
     *
     * @param item TransactionDetailItem
     * @return
     * @throws SQLException
     */
    private int purgeData(TransactionDetails.TransactionDetailItem item) throws SQLException{
        if (!item.resolvesForPrimaryKey) {
            return purgeData(item.getTableName(), item.getWhere());
        }
        else {
            // *** if it will be purged for PKids, chunk it since IN clause cannot take more than SQL_IN_CLAUSE_LIMIT
            int ret = 0;
            LongArrayIterator iter = new LongArrayIterator(item.getPrimaryKeyIds().toLongArray() , SQL_IN_CLAUSE_LIMIT);
            while (iter.hasNext()) {
                long[] ids = iter.next();
                if (ids == null || ids.length == 0) continue;
                String where = item.getPrimaryKeyColumnName() + " IN ("
                        + StringUtil.createCSVForNumber(new LongList(ids)) + ")";
                ret += purgeData(item.getTableName(), where);
            }
            return ret;
        }

    }

    /**
     * Purges data from given table with where clause.
     *
     * @param tableName
     * @param whereClause
     * @return
     * @throws SQLException
     */
    private int purgeData(String tableName, String whereClause) throws SQLException{

        Statement stm = null;
        int updatedRecords = 0;
        try {
            long s = System.currentTimeMillis();
            stm = connCore.createStatement();
            String sql = "DELETE FROM " + tableName + " WHERE " + whereClause;
            updatedRecords = stm.executeUpdate(sql);
            meterTime("Deleting data for : " + tableName , s);
        } finally{
            if (stm != null) stm.close();
        }
        return updatedRecords;
    }

    private void createConfigFile(String tableName, String whereClause , String tempPath)
            throws IOException, FileNotFoundException {

        String configXml = configXmlFormat.format(new String[] {tableName,
                XMLHelper.escapeXMLValue(whereClause)});
        File file = FileUtil.createFileWithDir(cfgFilePath);
        PrintWriter writer = new PrintWriter(new FileOutputStream(file));
        writer.println(configXml);
        writer.close();
    }

    private void deleteTempFiles(boolean logToTransaction) {
        // *** temp files areo nly used for DB mode
        if (!isArchiveModeDB) {
            return;
        }
        log("Deleting temp files");
        try {
            File fileCfg = new WBFile(cfgFilePath);
            File fileXml = new WBFile(xmlFilePath);
            if (fileCfg.exists()) {
                fileCfg.delete();
            }
            if (fileXml.exists()) {
                fileXml.delete();
            }
        }
        catch (Exception ex) {
            if (logToTransaction) {
                addTransactionDetailMessage(
                        "Temporary Files could not be deleted" +
                                JavaUtil.getStackTrace(ex));
            }
            else {
                log(JavaUtil.getStackTrace(ex));
            }
        }
    }

    /**
     * loadPolicy loads the policy for this archive/purge policy
     * @throws WBArchiveException thrown if an error occurs loading the policy
     */
    public void loadPolicy() throws WBArchiveException {
        archiveData.loadPolicy();
        purgeData.loadPolicy();
    }

    /**
     * Creates an archive transaction .
     *
     * @return
     * @throws WBArchiveException
     * @throws SQLException
     */
    public boolean createArchiveTransaction (boolean shouldCommit) throws SQLException {
        if (transData != null) {
            return true;
        }
        transData = new WBArchTransData ();
        transData.setWbapId(archivePolicyData.getWbapId() );
        transData.setWbatStart(new Date());
        transData.setWbatStatus(WBArchTransAccess.WBART_STATUS_PENDING);
        transData.setWbatAgingDate(this.getArchiveAgingDate());
        transData.setWbatRunParams(this.getArchivePolicyData().toParamDescription());
        WBArchTransAccess.createArchiveTransaction(getDBConnectionCore() , transData);
        if (shouldCommit) {
            getDBConnectionCore().commit();
        }
        /* cfg and file name is appended trans Id for concurrency */
        if (isArchiveModeDB) {
            xmlFilePath = tempFilePath + TEMP_FILE_PREFIX + transData.getWbatId() +
                    TEMP_FILE_XML_EXT;
            cfgFilePath = tempFilePath + TEMP_FILE_PREFIX + transData.getWbatId() +
                    TEMP_FILE_CFG_EXT;
        }

        return true;
    }

    /**
     * Finalizes an archive transaction with given message.
     *
     * @param msg
     * @return
     * @throws WBArchiveException
     * @throws SQLException
     */
    public boolean finalizeArchiveTransaction (String msg) throws WBArchiveException , SQLException{
        if (transData == null) {
            throw new WBArchiveException ("Transaction was not created successfully");
        }
        transData.setWbatEnd(new Date());
        transData.setWbatStatus(WBArchTransAccess.WBART_STATUS_APPLIED);
        transData.setWbatMsg(getTransactionRecordsAffected() + " total records have been processed successfully \n"
                + getTransactionMessage());
        WBArchTransAccess.updateArchiveTransaction(getDBConnectionCore() , transData );
        return true;
    }

    /**
     * Errors an archive transaction with given exception.
     *
     * @param t
     * @return
     * @throws WBArchiveException
     * @throws SQLException
     */
    public boolean errorArchiveTransaction (Throwable t) throws WBArchiveException , SQLException{
        String message = JavaUtil.getStackTrace( t );
        return errorArchiveTransaction(message);
    }

    /**
     * Appends msg to current transaction message.
     *
     * @param msg
     * @return
     */
    public boolean addToTransactionMessage(String msg) {
        transData.setWbatMsg(transData.getWbatMsg() + HTML_BREAK + msg);
        return true;
    }

    /**
     * Errors an archive transaction with given message.
     *
     * @param msg
     * @return
     * @throws WBArchiveException
     * @throws SQLException
     */
    public boolean errorArchiveTransaction (String msg) throws WBArchiveException , SQLException{
        // *** if transData == null, smt went wrong and rolledback
        if (transData == null) {
            createArchiveTransaction(false);
        }
        transData.setWbatEnd(new Date());
        transData.setWbatStatus(WBArchTransAccess.WBART_STATUS_ERROR);
        appendTransactionMessage(msg);
        transData.setWbatMsg(getTransactionRecordsAffected() + " total records have been processed successfully. \n"
                + " Errors occurred during processing  :\n"
                + getTransactionMessage());
        WBArchTransAccess.updateArchiveTransaction(getDBConnectionCore() , transData );
        return true;
    }

    /**
     * Appends msg to transaction message.
     *
     * @param msg
     */
    public void appendTransactionMessage(String msg) {
        transMsg.append(msg).append("\n");
    }

    /**
     * Returns current updated transaction message.
     *
     * @return
     */
    public String getTransactionMessage() {
        return transMsg.toString();
    }

    /**
     * Adds to the total number of records affected for this transaction.
     *
     * @param val
     */
    public void addTransactionRecordsAffected(int val) {
        totalRecordsAffected += val;
    }

    /**
     * Resets to the total number of records affected for this transaction.
     *
     */
    public void resetTransactionRecordsAffected() {
        totalRecordsAffected = 0;
    }

    /**
     * Returns the total number of records affected for this transaction.
     *
     * @return
     */
    public int getTransactionRecordsAffected() {
        return totalRecordsAffected;
    }

    /**
     * Returns current crated transaction data.
     *
     * @return
     */
    public WBArchTransData getTransactionData() {
        return transData;
    }

    /**
     * Notifies users based on policy definition.
     *
     * @throws SQLException
     */
    public void notifyUsers() {
        if (!StringUtil.isEmpty(getArchivePolicyData().getWbapNotify())) {
            if (getArchivePolicyData().requiresNotification(transData.getWbatStatus())) {
                try {
                    Message msg = new Message(getDBConnectionCore());
                    msg.setSenderId(WB_USER_ID);
                    msg.setTo(getArchivePolicyData().getWbapNotify());
                    msg.setSenderName(WB_USER_NAME);
                    msg.setMessageSubject("Archiving results");
                    StringBuilder bd = new StringBuilder(400);
                    bd.append(transData.getWbatMsg()).append(HTML_BREAK);
                    bd.append(" Status : ").append(transData.getWbatStatus()).append(HTML_BREAK);
                    bd.append(" Run params : ").append(transData.
                            getWbatRunParams());
                    msg.setMessageBody(bd.toString().substring(0,
                            Math.min(4000, bd.length())));
                    msg.setMessageType(com.workbrain.tool.mail.Util.
                            MESSAGE_TYPE_MAIL);
                    msg.send();
                    msg.closeAllStatements();
                    addToTransactionMessage("Notification sent to " + getArchivePolicyData().getWbapNotify());
                } catch (Exception e) {
                    logger.error("Could not send notification" , e);
                    addToTransactionMessage("Could not send notification but transaction completed. "
                            + " Error : " + e.getMessage());
                }
            } else {
                log ("No notification required");
            }

        }
    }

    protected long meterTime(String what, long start){
        long l = System.currentTimeMillis();
        log("\t"+what+" took: "+(l-start)+" millis");
        return l;
    }

    public void log( String message ) {
        if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) { logger.debug( message );}
    }

    private class TransactionDetails {
        private StringBuffer message = new StringBuffer(300);
        private List<TransactionDetailItem> transactionDetailItems;

        public String getMessage() {
            return message.toString();
        }

        public void addToMessage(String v) {
            message.append(v).append(HTML_BREAK)  ;
        }

        public void clearMessage() {
            message.setLength(0);
        }

        public List<TransactionDetailItem> getTransactionDetails() {
            return transactionDetailItems;
        }

        public void addToTransactionDetails(String tableName, String where) {
            TransactionDetailItem item = new TransactionDetailItem();
            item.setTableName(tableName);
            item.setWhere(where);
            if (transactionDetailItems == null) {
                transactionDetailItems = new ArrayList<TransactionDetailItem>();
            }
            transactionDetailItems.add(item);
        }

        /**
         * This is used when the where clause needs to be resolved to primary keys due
         * to constraints. When <code>commitWork</code> is executed such items will be executed
         * using generated primary key ids.
         *
         * @param tableName
         * @param where
         * @throws SQLException
         */
        public void addToTransactionDetailsResolvingToPrimaryKey(String tableName,
                                                                 String where) throws SQLException{

            TransactionDetailItem item = new TransactionDetailItem();
            item.setTableName(tableName);
            String pkColName = SchemaHelper.getPrimaryKeyColumnForTable(connCore, tableName);
            LongList ids = new LongList();
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                String sql = "SELECT " + pkColName + " FROM " + tableName
                        + (StringUtil.isEmpty(where) ? "" : " WHERE " + where);
                ps = connCore.prepareStatement(sql);
                rs = ps.executeQuery();
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                }
            }
            finally {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            }
            item.setPrimaryKeyIds(ids);
            item.setResolvesForPrimaryKey(true);
            item.setPrimaryKeyColumnName(pkColName);
            if (transactionDetailItems == null) {
                transactionDetailItems = new ArrayList<TransactionDetailItem>();
            }
            transactionDetailItems.add(item);
        }

        public void clearTransactionDetailItems() {
            if (transactionDetailItems != null) {
                transactionDetailItems.clear();
            }
        }

        public void clearAll() {
            clearMessage();
            clearTransactionDetailItems();
        }

        class TransactionDetailItem {
            private String tableName;
            private String where;
            private LongList pkIds;
            private boolean resolvesForPrimaryKey;
            private String primaryKeyColumnName;

            public String getTableName() {
                return tableName;
            }

            public void setTableName(String v) {
                tableName = v;
            }

            public String getWhere() {
                return where;
            }

            public void setWhere(String v) {
                where = v;
            }

            public LongList getPrimaryKeyIds() {
                return pkIds;
            }

            public void setPrimaryKeyIds(LongList v) {
                pkIds = v;
            }

            public void setResolvesForPrimaryKey(boolean v) {
                resolvesForPrimaryKey = v;
            }

            public String getPrimaryKeyColumnName() {
                return primaryKeyColumnName;
            }

            public void setPrimaryKeyColumnName(String v) {
                primaryKeyColumnName = v;
            }

        }
    }

    public void processArchiveSchemaPurge() throws WBArchiveException , SQLException{
        if (getArchivePolicyData().isCommitTypeEveryPolicyDetail()
                || getArchivePolicyData().isCommitTypeAllOrNothing()) {
            List<WBArchPolDetData> archDat = getArchivePolicyDetails();
            Iterator<WBArchPolDetData> iter = archDat.iterator();
            while (iter.hasNext()) {
                WBArchPolDetData archDet = iter.next();
                processPurgeArchivePolicyDetail(archDet,
                        getArchivePolicyData().isCommitTypeEveryPolicyDetail());
            }
            if (getArchivePolicyData().isCommitTypeAllOrNothing()) {
                commitWorkArchivePurge();
            }
        }
        else if (getArchivePolicyData().isCommitTypeEveryXDays()) {
            PurgeData.ArchivePolicyDayTokenizer tok = getArchivePolicyDayTokenizer_ForPurge();
            while (tok.hasNext()) {
                List<WBArchPolDetData> detailsThisBatch = tok.next();
                String log = "Processing days before " + tok.getCurrentAgingDate();
                log(log);
                addTransactionDetailMessage(log);
                processPurgeArchivePolicyDetails(detailsThisBatch, true);
            }

        }
        else if  (getArchivePolicyData().isCommitTypeEveryXEmployee()) {
            PurgeData.ArchivePolicyEmpIdTokenizer tok = getArchivePolicyEmpIdTokenizer_ForPurge();
            while (tok.hasNext()) {
                List<WBArchPolDetData> detailsThisBatch = tok.next();
                String log = "Processing " + tok.getCurrentBatchSize() + " employee(s)";
                log(log);
                addTransactionDetailMessage(log);
                processPurgeArchivePolicyDetails(detailsThisBatch, true);
            }
        }
        else if (getArchivePolicyData().isCommitTypeEveryXRecords()) {
            List<WBArchPolDetData> archDat = getArchivePolicyDetails();
            Iterator<WBArchPolDetData> iter = archDat.iterator();
            while (iter.hasNext()) {
                WBArchPolDetData archDet = iter.next();
                PurgeData.ArchivePolicyRecordTokenizer tok = getArchivePolicyRecordTokenizer_ForPurge(archDet);
                if (!tok.hasAny()) {
                    String log = "No records found for this policy detail";
                    log(log); addTransactionDetailMessage(log);
                }
                else {
                    while (tok.hasNext()) {
                        String sqlThisBatch = tok.next();
                        String log = "Processing BATCH SQL : \n" + sqlThisBatch;
                        log(log);  addTransactionDetailMessage(log);
                        WBArchPolDetData archDetThis = new WBArchPolDetData();
                        archDetThis.setWbapdTableName(archDet.getWbapdTableName());
                        archDetThis.setWbapdWhereClause(sqlThisBatch);
                        archDetThis.assignWbapdWhereClauseResolved(sqlThisBatch);
                        processPurgeArchivePolicyDetail(archDetThis, true);
                    }
                }
            }
        }
    }

    /**
     * Returns ArchivePolicyDayTokenizer for EVERY_X_DAYS archive policies
     * based on commit params.
     *
     * @return ArchivePolicyDayTokenizer
     */
    public PurgeData.ArchivePolicyDayTokenizer getArchivePolicyDayTokenizer_ForPurge(){
        return purgeData.getArchivePolicyDayTokenizer();
    }

    /**
     * Returns ArchivePolicyDayTokenizer for EVERY_X_EMPLOYEES archive policies
     * based on commit params.
     *
     * @return ArchivePolicyDayTokenizer
     */
    public PurgeData.ArchivePolicyEmpIdTokenizer getArchivePolicyEmpIdTokenizer_ForPurge() throws SQLException{
        return purgeData.getArchivePolicyEmpIdTokenizer();
    }

    /**
     * Returns ArchivePolicyDayTokenizer for EVERY_X_DAYS archive policies
     * based on commit params.
     *
     * @return ArchivePolicyRecordTokenizer
     */
    public PurgeData.ArchivePolicyRecordTokenizer getArchivePolicyRecordTokenizer_ForPurge
    (WBArchPolDetData det) throws SQLException{
        return purgeData.getArchivePolicyRecordTokenizer(det);
    }



    public void commitWorkArchivePurge() throws WBArchiveException , SQLException{
        List<TransactionDetails.TransactionDetailItem> transactionDetailItems = getTransactionDetails().getTransactionDetails();
        log("Processing " + (transactionDetailItems == null ? 0 :transactionDetailItems.size())
                + " transaction detail items");
        try {
            if (transactionDetailItems != null && transactionDetailItems.size() != 0) {
                Date start = new Date();
                for (int i = 0, k = transactionDetailItems.size(); i < k; i++) {
                    TransactionDetails.TransactionDetailItem item =
                            transactionDetailItems.get(i);
                    String str = item.getWhere();
                    /*if (str.indexOf("TO_DATE('04/30/2017 00:00:00','mm/dd/yyyy hh24:mi:ss')") > 0){
                        //Dodgy code here
                        str = str.replaceAll("'04/30/2017 00:00:00'","'02/28/2017 00:00:00'");
                        item.setWhere(str);
                    }*/

                    int affectedRecs = 0;
                    if ("Y".equals(archivePolicyData.getWbapFlag5())) {
                        affectedRecs = purgeArchiveData(item);
                    }
                    String msg = affectedRecs + " records have been deleted from Archive DB";
                    log(msg);  addTransactionDetailMessage(msg);
                    addTransactionRecordsAffected(affectedRecs);
                }
                addTransactionDetailMessage("Work Committed");

                /*deleteTempFiles(true);*/
                /*createArchiveTransactionDetails(connCore,
                        transactionDetailItems,
                        start,
                        getTransactionDetails().getMessage());*/
                if (isArchiveModeDB && archivePolicyData.isArchive()) {
                    connArch.commit();
                }
            }
            connCore.commit();
            log("Work Committed\n\n");
        } catch (Exception e){
            connCore.rollback();
            if (isArchiveModeDB && archivePolicyData.isArchive()) {
                connArch.rollback();
            }
            resetTransactionRecordsAffected();
            logger.error("Error in committing work", e);
            throw new WBArchiveException(e);
        } finally {
            getTransactionDetails().clearAll();
        }
    }


    /**
     * Processes list of resolved ArchivePolicyDetails.
     *
     * @param policyDetails
     * @param shouldCommit
     */
    public void processPurgeArchivePolicyDetails(List<WBArchPolDetData> policyDetails,
                                                 boolean shouldCommit)
            throws SQLException , WBArchiveException {
        if (policyDetails == null || policyDetails.size() == 0) {
            return;
        }

        Iterator<WBArchPolDetData> iter = policyDetails.iterator();
        while (iter.hasNext()) {
            WBArchPolDetData archDet = iter.next();
            processPurgeArchivePolicyDetail(archDet, false);
        }
        if (shouldCommit) {
            commitWorkArchivePurge();
        }

    }

    private void processPurgeArchivePolicyDetail(WBArchPolDetData archDet, boolean shouldCommit) throws WBArchiveException , SQLException{
        log("Processing archive detail for table " + archDet.getWbapdTableName());

        try {
            // Update Task Lock
            if (archiveTask != null){
                archiveTask.isInterrupted();
            }

            addTransactionDetailItem(archDet);
            if (shouldCommit) {
                commitWorkArchivePurge();
            }
        } catch(Exception e) {
            if (shouldCommit) {
                rollbackWork();
            }
            throw new WBArchiveException("Error in processing archive detail : "
                    + archDet.getWbapdTableName()  , e);
        }
    }

    private int purgeArchiveData(TransactionDetails.TransactionDetailItem item) throws SQLException{
        if (!item.resolvesForPrimaryKey) {
            return purgeArchiveData(item.getTableName(), item.getWhere());
        }else {
            // *** if it will be purged for PKids, chunk it since IN clause cannot take more than SQL_IN_CLAUSE_LIMIT
            int ret = 0;
            LongArrayIterator iter = new LongArrayIterator(item.getPrimaryKeyIds().toLongArray() , SQL_IN_CLAUSE_LIMIT);
            while (iter.hasNext()) {
                long[] ids = iter.next();
                if (ids == null || ids.length == 0) continue;
                String where = item.getPrimaryKeyColumnName() + " IN ("
                        + StringUtil.createCSVForNumber(new LongList(ids)) + ")";
                ret += purgeArchiveData(item.getTableName(), where);
            }
            return ret;
        }
    }

    /**
     * Purges data from given table with where clause.
     *
     * @param tableName
     * @param whereClause
     * @return
     * @throws SQLException
     */
    private int purgeArchiveData(String tableName, String whereClause) throws SQLException{

        Statement stm = null;
        int updatedRecords = 0;
        try {
            long s = System.currentTimeMillis();
            stm = connArch.createStatement();
            String sql = "DELETE FROM " + tableName + " WHERE " + whereClause;
            updatedRecords = stm.executeUpdate(sql);
            meterTime("Deleting data for : " + tableName , s);
        } finally{
            if (stm != null) stm.close();
        }
        return updatedRecords;
    }
}

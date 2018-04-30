package com.itella.app.wbarchive;

import java.sql.*;
import java.util.*;

import com.workbrain.app.ta.db.JobAccess;
import com.workbrain.app.ta.db.StJobSkillAccess;
import com.workbrain.app.ta.model.JobData;
import com.workbrain.app.ta.model.StJobSkillData;
import com.workbrain.app.wbarchive.*;
import com.workbrain.app.wbarchive.model.*;
import com.workbrain.sql.DBConnection;
import com.workbrain.util.StringUtil;
/**
 * Custom class for archiving expired skills.
 * Due to cross dependency between ST_JOB_SKILL and ST_SKILL tables,
 * (i.e policies depend on ST_JOB_SKILL  records) ST_JOB_SKILL should be deleted last
 * 
 */
public class ItellaArchiveExpiredSkills extends WBArchiveComponent {

    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ItellaArchiveExpiredSkills.class);

    private static final String TABLE_ST_JOB_SKILL = "ST_JOB_SKILL";
    private static final String TABLE_ST_SKILL = "ST_SKILL";    

    //private WBArchive wbarchive;

    public void processArchive(WBArchive wbarchive) throws WBArchiveException , SQLException{

        if (!(wbarchive.getArchivePolicyData().isCommitTypeAllOrNothing() || wbarchive.getArchivePolicyData().isCommitTypeEveryXDays())) {
            throw new RuntimeException("Commit type must be " + WBArchPolicyData.COMMIT_TYPE_ALL_OR_NOTHING + " or " + WBArchPolicyData.COMMIT_TYPE_EVERY_X_DAYS);
        }

        //this.wbarchive = wbarchive;
        if (wbarchive.getArchivePolicyData().isCommitTypeAllOrNothing()){
	        processArchivePolicyDetails(wbarchive, wbarchive.getArchivePolicyDetails());
        }else{
            ArchiveData.ArchivePolicyDayTokenizer tok = wbarchive.getArchivePolicyDayTokenizer();
            while (tok.hasNext()) {
                List detailsThisBatch = (List) tok.next();
                String log = "Processing days before " + tok.getCurrentAgingDate();
                log(log);
                wbarchive.addTransactionDetailMessage(log);
                processArchivePolicyDetails(wbarchive, detailsThisBatch);
            }
        }

    }


    /**
     * @param wbarchive
     * @throws WBArchiveException
     * @throws SQLException
     */
    private void processArchivePolicyDetails(WBArchive wbarchive, List archDat) throws WBArchiveException, SQLException {
        if (archDat == null) return;
        DBConnection conn = wbarchive.getDBConnectionCore();
        List<WBArchPolDetData> afterPolDets = new ArrayList<WBArchPolDetData>();
        Iterator iter = archDat.iterator();
        while (iter.hasNext()) {
            WBArchPolDetData archDet = (WBArchPolDetData) iter.next();
            if (TABLE_ST_JOB_SKILL.equalsIgnoreCase(archDet.getWbapdTableName())
                    || TABLE_ST_SKILL.equalsIgnoreCase(archDet.getWbapdTableName())) {
                int recCnt = wbarchive.getCountForTableWhereClause(archDet.getWbapdTableName(), 
                        archDet.retrieveWbapdWhereClauseResolved());
                if (recCnt > 0) {
                    archDet.assignResolvesForPrimaryKey(true);
                    afterPolDets.add(archDet);
                }
            }
            else {
                wbarchive.processArchivePolicyDetail(
                    archDet,
                    false);
            }
        }
        Iterator<WBArchPolDetData> iterAfter = afterPolDets.iterator();
        Set<Long> jobsAffected = new HashSet<Long>();
        StJobSkillAccess jsa = new StJobSkillAccess(conn);
        while (iterAfter.hasNext()) {
            WBArchPolDetData item = iterAfter.next();
            // If some job skills are going to be archived, then retrieve the details
            // so that the jobs can be later checked for any skills at all
            if (TABLE_ST_JOB_SKILL.equalsIgnoreCase(item.getWbapdTableName())) {
                List<StJobSkillData> jsdsArchived = jsa.loadRecordData(new StJobSkillData(), 
                                                                       item.getWbapdTableName(),  
                                                                       item.retrieveWbapdWhereClauseResolved());
                
                if (jsdsArchived != null && jsdsArchived.size() > 0){
                    for (StJobSkillData jsd : jsdsArchived){
                        jobsAffected.add(jsd.getJobId());                     
                    }
                }
            }
            wbarchive.processArchivePolicyDetail(
                                item,
                                false);
            
        }
        wbarchive.commitWork();
        updateJobsWithNoSkills(conn, jobsAffected);
    }


    /**
     * Updates the flag "Exclude From Qualification Updates" to "Y"
     * when the job has no skills, to ensure that it's not assigned to 
     * all employees
     * 
     * @param conn
     * @param jobsAffected
     */
    public void updateJobsWithNoSkills(DBConnection conn,
                                       Set<Long> jobsAffected) {
        List<Long> jobsWithNoSkills = new ArrayList<Long>();
        StJobSkillAccess jsa = new StJobSkillAccess(conn);
        for (Long jobId : jobsAffected){
            List<StJobSkillData> jsdsAfterArch = jsa.loadByJob(jobId);
            
            if (jsdsAfterArch != null && jsdsAfterArch.size() == 0){
                jobsWithNoSkills.add(jobId);                    
            }
        }
        
        JobAccess ja = new JobAccess(conn);
        for (Long jobWnsId : jobsWithNoSkills){
            JobData job = ja.load(jobWnsId);
            
            if (!StringUtil.isEmpty(job.getJobExcFrmQlfy()) && 
                job.getJobExcFrmQlfy().equals("N")){
                job.setJobExcFrmQlfy("Y");
                ja.updateRecordData(job, JobAccess.JOB_TABLE, JobAccess.JOB_PRI_KEY);
            }
        }
    }

    /**
     * Purge not implemented.
     * @return
     */
    //public boolean isTypePurgeImplemented() {
    //    return false;
    //}

}

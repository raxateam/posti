package com.itella.app.wbarchive;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.workbrain.app.wbarchive.WBArchiveDefault;
import com.workbrain.sql.DBConnection;
import com.workbrain.sql.SQLUtil;
/**
 * Extension class for itella archiving data.
 * 
 */
public class AbtractItellaArchiveData extends WBArchiveDefault {


	/**
	 * Executes SQL with no params and no clientIds
	 * @param conn
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
    protected int executeSQL(DBConnection conn, String sql) throws SQLException {
    	PreparedStatement ps = null;
    	int cnt = 0;
		try {
			ps = conn.prepareStatement(sql,true, true);
			cnt = ps.executeUpdate();
		} 
		finally {
			SQLUtil.cleanUp(ps);
		}
		return cnt;
    }

}

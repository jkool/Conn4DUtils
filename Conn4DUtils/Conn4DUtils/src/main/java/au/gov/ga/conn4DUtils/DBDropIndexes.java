package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

public class DBDropIndexes extends DBProcessTableSet{

	public String tag = "cmr";
	
	public static void main(String[] args) {
		DBDropIndexes ddi = new DBDropIndexes();
		ddi.connect();
		ddi.batchProcess("sw_201.*_cmr.*\\d{1,2}$");
		ddi.close();
		System.out.println("Complete");
	}
	
	public void process(String name) throws SQLException {

		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);
		System.out.println("\tDropping indexes...\t\t(" + new Date(System.currentTimeMillis())+")");
		if(indexExists(name,name+"_cmr_start_geom")){
			stmt.execute("DROP INDEX " + name + "_cmr_start_geom");
		}
		if(indexExists(name,name+"_cmr_start_relid")){
			stmt.execute("DROP INDEX " + name + "_cmr_start_relid");
		}
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
}

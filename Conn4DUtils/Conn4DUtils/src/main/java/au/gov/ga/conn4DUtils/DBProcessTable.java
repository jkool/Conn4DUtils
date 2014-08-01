package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

public abstract class DBProcessTable extends DBProcessTableSet {

	protected boolean overwrite = true;
	
	public void process(String name) throws SQLException {

		boolean exists = tableExists(getOutputName(name)); 
		
		if (exists && !overwrite) {
			System.out.println(name + " has been processed.  Skipping.");
			return;
		}

		stmt = conn.createStatement();
		
		if (exists){
			stmt.execute("DROP VIEW " + getOutputName(name));
		}

		long start = System.currentTimeMillis();
		stmt.execute("SET work_mem='4GB'");
		System.out.print("\tCreating table "+getOutputName(name)+"...\t\t(" + new Date(System.currentTimeMillis())+")");
		stmt.execute(getSQL(name));
		long stop = System.currentTimeMillis();
		System.out.println("\tComplete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
	
	public void setOverwrite(boolean overwrite){
		this.overwrite = overwrite;
	}
	
	protected abstract String getSQL(String name);
	
	protected abstract String getOutputName(String name);
}

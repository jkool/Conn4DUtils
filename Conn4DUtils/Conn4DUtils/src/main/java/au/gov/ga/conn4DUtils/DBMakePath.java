package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

public class DBMakePath extends DBProcessTableSet{

	public String tag = "kef";
	
	public static void main(String[] args) {
		DBMakePath ddmp = new DBMakePath();
		ddmp.connect();
		ddmp.batchProcess("n_.*d\\d{1,2}$");
		//ddmp.batchProcess("nw.*d\\d{1,2}$");
		//ddmp.batchProcess("nn_.*d\\d{1,2}$");
		//ddmp.batchProcess("t2009_01_01d0$");
		ddmp.close();
		System.out.println("Complete");
	}
	
	public void process(String name) throws SQLException {

		if (tableExists(name + "_"+tag+"_path")){
			System.out.println(name + " has been processed.  Skipping.");
			return;
		}
		
		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);
		System.out.println("\tSelecting matching paths\t" + df.format(new Date(System.currentTimeMillis())));
		//stmt.execute("CREATE VIEW " + name + "_"+tag+"_path AS SELECT t1.*,t2.Network,t2.MPA_NAME,t2.ZONE,t2.IUCN,t2.Label,t2.Area_km2,t2.Legend FROM " + name + " t1 INNER JOIN " + name + "_"+tag+"_start t2 ON (t1.relid=t2.relid)");
		//stmt.execute("CREATE VIEW " + name + "_"+tag+"_path AS SELECT t1.*,t2.sort FROM " + name + " t1 INNER JOIN " + name + "_"+tag+"_start t2 ON (t1.relid=t2.relid)");
		stmt.execute("CREATE VIEW " + name + "_"+tag+"_path AS SELECT t1.*,t2.name,t2.region FROM " + name + " t1 INNER JOIN " + name + "_"+tag+"_start t2 ON (t1.relid=t2.relid)");
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
}

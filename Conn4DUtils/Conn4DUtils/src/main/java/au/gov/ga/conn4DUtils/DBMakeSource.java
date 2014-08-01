package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

public class DBMakeSource extends DBProcessTableSet {

	private String tag = "kef";
	//private String shapefile = tag + "_sw_g";/////////////////////////////
	private String shapefile = tag + "_nw";/////////////////////////////

	public static void main(String[] args) {
		DBMakeSource src = new DBMakeSource();
		src.connect();
		//src.batchProcess("nw_.*d\\d{1,2}$");
		src.batchProcess("n_.*d\\d{1,2}$");
		//src.batchProcess("nn_.*d\\d{1,2}$");
		//src.batchProcess("nn_2009_01_01d1$");
		//src.batchProcess("t2009_01_01d0$");
		src.close();
		System.out.println("Complete");
	}

	public void process(String name) throws SQLException {

		if (tableExists(name + "_"+tag+"_start")) {
			System.out.println(name + " has been processed.  Skipping.");
			return;
		}

		System.out.println(">> Working on " + name);
		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		stmt.execute("SET work_mem='4GB'");
		System.out.println("\tCreating the table...\t\t(" + df.format(new Date(System.currentTimeMillis()))+")");
		stmt.execute("DROP VIEW IF EXISTS " + name + "_" + tag + "_start");
		System.out.println("\tFiltering origin points\t\t"
				+ df.format(new Date(System.currentTimeMillis())));
		String sql = "CREATE VIEW "
				+ name
				+ "_"
				+ tag
				//+ "_start AS SELECT t1.*,t2.Network,t2.MPA_NAME,t2.ZONE,t2.IUCN,t2.Label,t2.Area_km2,t2.Legend FROM "
				//+ "_start AS SELECT t1.*,t2.sort FROM "
				+ "_start AS SELECT t1.*,t2.name,t2.region FROM "
				+ name + " t1 INNER JOIN " + shapefile
				+ " t2 ON ST_Intersects(t1.geom, t2.geom) WHERE t1.duration=0";
		stmt.execute(sql);
		
		System.out.println("\tGenerating indexes...\t\t"
				+ df.format(new Date(System.currentTimeMillis())));
		//stmt.execute("CREATE INDEX " + name + "_" + tag +"_start_relid ON " + name + "_" + tag +"_start(relid)");
		//stmt.execute("CREATE INDEX " + name + "_" + tag +"_start_geom ON " + name + "_" + tag +"_start(geom)");
		
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
}
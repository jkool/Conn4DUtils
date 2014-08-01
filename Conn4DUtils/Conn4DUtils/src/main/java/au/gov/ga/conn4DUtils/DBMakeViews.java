package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

public class DBMakeViews extends DBProcessTableSet{

	public String tag = "cmr";
	private String shapefile = tag + "_nw_g";
	private String dest_layer = tag + "_nw_g";
	private boolean overwrite = true;
	
	public static void main(String[] args){
		DBMakeViews dbv = new DBMakeViews();
		dbv.connect();
		dbv.batchProcess("^sw_2011_07_20d1$");
		//dbo.batchProcess("nn_.*d\\d{1,2}$");
		//dbo.batchProcess("nn_2009_01_01d1$");
		//dbo.batchProcess("t2009.*d\\d{1,2}$");
		//dbo.batchProcess("test_\\d{1}$");
		dbv.close();
		System.out.println("Complete");
	}
	
	public void process(String name) throws SQLException{
		
		DBProcessOrigin dbo = new DBProcessOrigin();
		dbo.setOverwrite(overwrite);
		dbo.process(name);
		
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);
		stmt.execute("SET work_mem='4GB'");
		System.out.println("\tFinding origin points\t\t" + df.format(new Date(System.currentTimeMillis())));
		stmt.execute("CREATE VIEW " + name + "_o AS SELECT * FROM " + name + " WHERE duration=0;");
		System.out.println("\tFiltering origin points\t\t"
				+ df.format(new Date(System.currentTimeMillis())));
		String sql = "CREATE VIEW "
				+ name
				+ "_"
				+ tag
				+ "_start AS SELECT t1.*,t2.Network,t2.MPA_NAME,t2.ZONE,t2.IUCN,t2.Label,t2.Area_km2,t2.Legend FROM "
				+ name + " t1 INNER JOIN " + shapefile
				+ " t2 ON ST_Intersects(t1.geom, t2.geom) WHERE t1.duration=0";
		stmt.execute(sql);
		stmt.execute("CREATE VIEW "
				+ name
				+ "_"+tag+"_dst AS SELECT t1.*,t2.Network,t2.MPA_NAME,t2.ZONE,t2.IUCN,t2.Label,t2.cmr_status,t2.Area_km2,t2.Legend FROM "
				+ name + " t1 INNER JOIN " + dest_layer
				+ " t2 ON ST_Intersects(t1.geom,t2.geom)");
		stmt.execute("CREATE VIEW " + name + "_"+tag+"_path AS SELECT t1.*,t2.Network,t2.MPA_NAME,t2.ZONE,t2.IUCN,t2.Label,t2.Area_km2,t2.Legend FROM " + name + " t1 INNER JOIN " + name + "_"+tag+"_start t2 ON (t1.relid=t2.relid)");
		stmt.execute("CREATE VIEW "
				+ name
				+ "_"
				+ tag
				+ "_rev AS SELECT t1.*,t2.Network,t2.MPA_NAME,t2.ZONE,t2.IUCN,t2.Label,t2.cmr_status,t2.Area_km2,t2.Legend FROM "
				+ name
				+ " t1 INNER JOIN (SELECT relid,MAX(duration) duration,MAX(network) network, MAX(mpa_name) mpa_name, MAX(\"zone\") \"zone\", MAX(iucn) iucn, MAX(\"label\") \"label\", MAX(cmr_status) cmr_status, MAX(area_km2) area_km2, MAX(legend) legend FROM "
				+ name
				+ "_"
				+ tag
				+ "_dst GROUP BY relid) t2 ON t1.relid=t2.relid WHERE t1.duration<t2.duration");
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: " + TimeConvert.millisToString(stop-start));
	}

}

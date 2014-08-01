package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

public class DBMakeSurface extends DBProcessTableSet {

	//private String shapefile = "kef_nw_g";
	//private String tag = "kef";
	
	private String shapefile = "kef_nw_g";
	private String tag = "kef";
	
	public static void main(String[] args){
		DBMakeSurface dbm = new DBMakeSurface();
		dbm.connect();
		dbm.batchProcess("t.*d0$");
		dbm.close();
		System.out.println("Complete");
	}
	
	public void process(String name) throws SQLException{
		
		if(name.contains("_"+tag+"_path") || name.contains("_o") || name.contains(tag+"_start")){
			return;
		}
		
		if (tableExists(name + "_"+tag+"_path")){
			System.out.println(name + " has been processed.  Skipping.");
			return;
		}
		
		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);
		stmt.execute("SET work_mem='4GB'");
		//stmt.execute("DROP TABLE IF EXISTS " + name + "_o");
		//System.out.println("\tFinding origin points\t\t" + df.format(new Date(System.currentTimeMillis())));
		stmt.execute("CREATE TABLE " + name + "_o AS SELECT * FROM " + name + " WHERE duration=0;");
		//System.out.println("\tCreating indexes\t\t" + df.format(new Date(System.currentTimeMillis())));
		//stmt.execute("CREATE INDEX " + name + "_o_relid ON "+name+"_o(relid)");
		//stmt.execute("CREATE INDEX " + name + "_o_geom ON "+name+"_o(geom)");
		stmt.execute("DROP TABLE IF EXISTS " + name + "_"+tag+"_start");
		System.out.println("\tFiltering origin points\t\t" + df.format(new Date(System.currentTimeMillis())));
		//**stmt.execute("CREATE TABLE " + name + "_"+tag+"_start AS SELECT "+name+"_o.* FROM "+name+"_o INNER JOIN "+shapefile+" ON ST_Intersects("+name+"_o.geom, ST_Transform("+shapefile+".geom,4326))");
		stmt.execute("CREATE TABLE " + name + "_"+tag+"_start AS SELECT t1.*, t2.Network,t2.MPA_NAME,t2.ZONE,t2.IUCN,t2.Label,t2.cmr_status,t2.Area_km2,t2.Legend FROM "+name+"_o t1 INNER JOIN "+shapefile+" t2 ON ST_Intersects(t1.geom, t2.geom)");
		stmt.execute("DROP TABLE IF EXISTS " + name + "_"+tag+"_path");
		System.out.println("\tSelecting matching paths\t" + df.format(new Date(System.currentTimeMillis())));
		stmt.execute("CREATE TABLE " + name + "_"+tag+"_path AS SELECT " + name + ".* FROM " + name + " INNER JOIN " + name + "_"+tag+"_start ON ("+name+".relid="+name+"_"+tag+"_start.relid)");
		
		System.out.println("\tGenerating indexes...\t\t"
				+ df.format(new Date(System.currentTimeMillis())));
		stmt.execute("CREATE INDEX " + name + "_" + tag +"_start_relid ON " + name + "(relid)");
		stmt.execute("CREATE INDEX " + name + "_" + tag +"_start_geom ON " + name + "(geom)");
		
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: " + TimeConvert.millisToString(stop-start));
	}
}

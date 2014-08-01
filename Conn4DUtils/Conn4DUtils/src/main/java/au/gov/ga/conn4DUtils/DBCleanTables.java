package au.gov.ga.conn4DUtils;

import java.sql.SQLException;

public class DBCleanTables extends DBProcessTableSet {

	private String fieldName = "relid";
	public boolean overwrite = false;

	public static void main(String[] args) {
		DBCleanTables dct = new DBCleanTables();
		dct.connect();
		// ddt.batchProcess("^nw_2.*d(2[1-9]|3[0-1])$");
		//dct.batchProcess("^nw_.*d\\d{1,2}$");
		dct.batchProcess("^sw_2012.*");
		//dct.batchProcess("^sw_2011.*");
		//dct.batchProcess("^sw_2010_(0[7-9]|1[0-2]).*");
		//dct.batchProcess("^sw_2010_07_25d([2-9]|2[0-9]|3[0-1])$");
		dct.close();
		System.out.println("Complete");
	}

	public void process(String name) throws SQLException {

		stmt = conn.createStatement();
		long start = System.currentTimeMillis();

		System.out.println("\n>> Working on " + name + "\t" + "("
				+ new java.util.Date(start).toString() + ")");

		boolean exists = indexExists(name, name + "_pkey"); 
		
		if(exists){
			if(overwrite){
				stmt.execute("ALTER TABLE " + name + " DROP CONSTRAINT " + name + "_pkey");
				exists=false;
			}
		}
		
		if (!exists) {
			
			if(fieldExists(name,fieldName)){
				stmt.execute("ALTER TABLE " + name + " DROP COLUMN relid CASCADE");
			}
			
			if(fieldExists(name,"key")){
				stmt.execute("ALTER TABLE " + name + " DROP COLUMN key");
			}
			
			if(indexExists(name, name+"_depth")){
				stmt.execute("DROP INDEX " + name + "_depth");
			}
			
			if(indexExists(name, name+"_destination")){
				stmt.execute("DROP INDEX " + name + "_destination");
			}

			if(indexExists(name, name+"_duration")){
				stmt.execute("DROP INDEX " + name + "_duration");
			}

			if(indexExists(name, name+"_geom")){
				stmt.execute("DROP INDEX " + name + "_geom");
			}

			if(indexExists(name, name+"_lat")){
				stmt.execute("DROP INDEX " + name + "_lat");
			}

			if(indexExists(name, name+"_lon")){
				stmt.execute("DROP INDEX " + name + "_lon");
			}

			if(indexExists(name, name+"_source")){
				stmt.execute("DROP INDEX " + name + "_source");
			}

			if(indexExists(name, name+"_time")){
				stmt.execute("DROP INDEX " + name + "_time");
			}
			
			if(indexExists(name, name + "_relid")){
				stmt.execute("DROP INDEX " + name + "_relid");
			}

			DBAddPathID dbi = new DBAddPathID();
			dbi.connect();
			dbi.process(name);
			//DBMakeViews dmv = new DBMakeViews();
			//dmv.connect();
			//dmv.process(name);
			System.out.println("Vacuuming...");
			stmt.execute("VACUUM ANALYZE " + name);
			dbi.close();
			//dmv.close();
		}
		else{System.out.println("Skipping");}
	}
}
package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

public class DBDropTables extends DBProcessTableSet{
	
	public static void main(String[] args) {
		DBDropTables ddt = new DBDropTables();
		ddt.connect();
		//ddt.batchProcess("^nw_2.*d(2[1-9]|3[0-1])$");
		ddt.batchProcess("^sw_2012.*");
		ddt.close();
		System.out.println("Complete");
	}
	
	public void process(String name) throws SQLException {

		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);
		//stmt.execute("VACUUM ANALYZE " + name);
		//stmt.execute("ALTER VIEW " + name + " RENAME TO " + "nw_" + name.substring(1));
		//System.out.println("\tDropping table...\t\t(" + new Date(System.currentTimeMillis())+")");
		//System.out.println("\tRenaming table to "+"nw_"+name.substring(1)+"...\t\t(" + new Date(System.currentTimeMillis())+")");
		//stmt.execute("DROP VIEW " + name);
		//stmt.execute("DROP TABLE " + name + " CASCADE");
		//if(!indexExists(name,name + "_pkey")){
			//System.out.println("OK");
			//stmt.execute("ALTER TABLE " + name + " ADD PRIMARY KEY(source,id,time_)");
		//}
		//stmt.execute("ALTER TABLE " + name + " RENAME TO " + "nw_" + name.substring(1));
		/*if(!indexExists(name,name +"_time")){
			stmt.execute("CREATE INDEX " + name + "_time ON " + name +"(time_)");
			stmt.execute("CREATE INDEX " + name + "_depth ON " + name +"(depth)");
			stmt.execute("CREATE INDEX " + name + "_lat ON " + name +"(lat)");
			stmt.execute("CREATE INDEX " + name + "_lon ON " + name +"(lon)");
			stmt.execute("CREATE INDEX " + name + "_duration ON " + name +"(duration)");
			stmt.execute("CREATE INDEX " + name + "_source ON " + name +"(source)");
			stmt.execute("CREATE INDEX " + name + "_destination ON " + name +"(destination)");
			stmt.execute("CREATE INDEX " + name + "_geom ON " + name +"(geom)");
		}*/
		
		//if(indexExists(name,name+"_pkey")&&!indexExists(name,name+"_relid")){
			//System.out.println("Relid index missing...");
			//stmt.execute("ALTER TABLE " + name + " ADD PRIMARY KEY (source,id,time_)");
			//stmt.execute("CREATE INDEX " + name + "_relid ON " + name + "(relid)"); 
		//}
		
		//else{
			//System.out.println("Skipping...");
		//}
		
		//if(!indexExists(name,name+"_relid")){
		//	System.out.print("Relid index is missing from " + name + ".  Creating index...   ");
		//	stmt.execute("CREATE INDEX " + name + "_relid ON " + name + "(relid)");
		//	System.out.println("Done!");
		//}
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
}
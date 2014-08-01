package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

/**
 * 
 * Adds a unique identifier to each path in the database using a combination
 * of source name and id.  Also performs clean-up and indexing.
 * 
 * @author Johnathan Kool
 *
 */

public class DBAddPathID extends DBProcessTableSet {

	private String fieldName = "relid";
	private Statement stmt;

	public static void main(String[] args) {
		DBAddPathID did = new DBAddPathID();
		did.connect();
		//did.batchProcess("sw_2009.*d\\d{1,2}$");
		did.batchProcess("^sw_2010_07_25d0$");//.*d\\d{1,2}$");
		//did.batchProcess("sw_.*d\\d{1,2}$");
		//did.batchProcess("test_\\d{1}$");
		did.close();
		System.out.println("Complete");
	}
	
	/**
	 * Adds the path id to the table provided as a String
	 */

	public void process(String name) throws SQLException {
		if (this.fieldExists(name, fieldName)) {
			System.out.println(name
					+ " already contains field '"+fieldName+"'.  Skipping.");
		}
		else {
			long start = System.currentTimeMillis();
			long t1,t2;
			
			stmt = conn.createStatement();
			stmt.execute("SET work_mem='4GB'");
			
			stmt.execute("DROP TABLE IF EXISTS " + name + "_rel");
			
			// Get distinct values
			
			System.out.printf("\t%-50s%-30s","Finding distinct values", new Date(System.currentTimeMillis()));
			t1 = System.currentTimeMillis();
			stmt.execute("CREATE TABLE " + name
					+ "_rel AS SELECT source,id FROM " + name
					+ " GROUP BY source,id");
			t2 = System.currentTimeMillis();
			System.out.printf("(%-15s)%n", TimeConvert.millisToString(t2 - t1));
			
			// Drop the _key table if it already exists
			
			stmt.execute("DROP TABLE IF EXISTS " + name + "_key");
			
			// Order and number the distinct values
			
			System.out.printf("\t%-50s%-30s","Ordering and numbering distinct values",new Date(System.currentTimeMillis()));
			t1 = System.currentTimeMillis();
			stmt.execute("CREATE TABLE "
					+ name
					+ "_key AS SELECT row_number() over (ORDER BY source,id) AS "+fieldName+", source, id FROM "
					+ name + "_rel");
			t2 = System.currentTimeMillis();
			System.out.printf("(%-15s)%n", TimeConvert.millisToString(t2 - t1));
			
			// Generate indexes on the distinct values to save time
			
			System.out.printf("\t%-50s%-30s","Generating indexes on distinct values",new Date(System.currentTimeMillis()));
			t1 = System.currentTimeMillis();
			stmt.execute("CREATE INDEX " + name + "_key_source ON " + name
					+ "_key(source)");
			stmt.execute("CREATE INDEX " + name + "_key_id ON " + name
					+ "_key(id)");
			t2 = System.currentTimeMillis();
			System.out.printf("(%-15s)%n", TimeConvert.millisToString(t2 - t1));
			
			// Join the path ID to the table
			
			System.out.printf("\t%-50s%-30s","Joining unique path ID to the original table",new Date(System.currentTimeMillis()));
			stmt.execute("DROP TABLE IF EXISTS " + name + "_tmp");
			t1 = System.currentTimeMillis();
			stmt.execute("CREATE TABLE " + name
					+ "_tmp AS SELECT t1.*,t2."+fieldName+" FROM " + name
					+ " t1 INNER JOIN " + name
					+ "_key t2 ON t1.source=t2.source AND t1.id=t2.id");
			stmt.execute("ALTER TABLE " + name + "_tmp ADD COLUMN key serial");
			t2 = System.currentTimeMillis();
			System.out.printf("(%-15s)%n", TimeConvert.millisToString(t2 - t1));
			
			// Remove any duplicate entries
			
			System.out.printf("\t%-50s%-30s","Finding and eliminating duplicates",new Date(System.currentTimeMillis()));
			t1 = System.currentTimeMillis();
			stmt.execute("DELETE FROM "
					+ name
					+ "_tmp t1 USING "
					+ name
					+ "_tmp t2 WHERE t1."+fieldName+"=t2."+fieldName+" AND t1.time_=t2.time_ AND t1.key<t2.key");
			t2 = System.currentTimeMillis();
			System.out.printf("(%-15s)%n", TimeConvert.millisToString(t2 - t1));
			
			// Replace the original table with the new one
			
			System.out.printf("\t%-50s%-30s","Replacing original table with new table",new Date(System.currentTimeMillis()));
			t1 = System.currentTimeMillis();
			if (!tableExists(name + "_tmp")) {
				stmt.execute("ALTER TABLE " + name + " RENAME TO " + name
						+ "_X");
				System.out
						.println("\n\t\t" +name
								+ "_tmp was not created.  DROP operation and post-processing skipped.");
			} else {
				stmt.execute("DROP TABLE " + name + " CASCADE");
				stmt.execute("ALTER TABLE " + name + "_tmp RENAME TO " + name);
				stmt.execute("DROP TABLE " + name + "_rel");
				stmt.execute("DROP TABLE " + name + "_key");
				t2 = System.currentTimeMillis();
				System.out.printf("(%-15s)%n", TimeConvert.millisToString(t2 - t1));
				
				// Generate the primary key
				
				System.out.printf("\t%-50s%-30s","Adding primary key",new Date(System.currentTimeMillis()));
				t1 = System.currentTimeMillis();
				stmt.execute("ALTER TABLE " + name
						+ " ADD PRIMARY KEY (source,id,time_)");
				t2 = System.currentTimeMillis();
				System.out.printf("(%-15s)%n", TimeConvert.millisToString(t2 - t1));				
				System.out.println();
				
				// Add indexes using the indexer
				
				DBIndexer dbidx = new DBIndexer();
				dbidx.setConnection(conn);
				dbidx.process(name);
				dbidx.addIndex(name, fieldName);
				long stop = System.currentTimeMillis();
				System.out.println();
				System.out.println(">> " + name + " complete.  Elapsed time: "
						+ TimeConvert.millisToString(stop - start) + "\n");
			}
		}
	}
}

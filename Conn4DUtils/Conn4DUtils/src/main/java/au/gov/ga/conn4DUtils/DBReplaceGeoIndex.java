package au.gov.ga.conn4DUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBReplaceGeoIndex extends DBProcessTableSet {

	public static void main(String[] args) {
		DBReplaceGeoIndex drg = new DBReplaceGeoIndex();
		drg.connect();
		drg.batchProcess("^nw_201.*\\d{1,2}$");
		System.out.println("Complete");
	}

	public void process(String table) throws SQLException {
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + table);
		Statement stmt = conn.createStatement();
		stmt.execute("DROP INDEX " + table + "_geom");
		stmt.execute("CREATE INDEX " + table + "_geom ON " + table + " USING GIST (geom)");
		stmt.execute("VACUUM ANALYZE " + table);
		long stop = System.currentTimeMillis();
		System.out.println(">> " + table + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
}

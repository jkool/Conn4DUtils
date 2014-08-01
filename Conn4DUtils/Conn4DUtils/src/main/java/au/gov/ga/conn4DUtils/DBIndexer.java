package au.gov.ga.conn4DUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DBIndexer extends DBProcessTableSet {

	private boolean overwrite = true;

	public static void main(String[] args) {
		DBIndexer dbi = new DBIndexer();
		dbi.connect();
		dbi.batchProcess("^sw_2010_(0[7-9]|1[0-2]).*");
		System.out.println("Complete");
	}
	
	public void process(String table) {
		try {
			if (tableExists(table)) {
				stmt = conn.createStatement();
				System.out.println("Creating indexes on " + table + "...");
				
				addIndex(table,"time_");
				addIndex(table,"duration");
				addIndex(table,"source");
				addIndex(table,"destination");
				addSpatialIndex(table,"geom");
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void indexTables(Date fromDate, Date toDate, int interval,
			String units, int fromD, int toD) {
		try {
			Statement stmt = conn.createStatement();

			DateFormat df = new SimpleDateFormat("yyyy_MM_dd");
			long fromT = fromDate.getTime();
			long toT = toDate.getTime();
			long isec = TimeConvert.convertToMillis(units, interval);

			for (long i = fromT; i < toT; i += isec) {
				for (int j = fromD; j < toD; j++) {
					String table = "t" + df.format(new Date(i)) + "d" + j;
					indexTable(table);
				}
			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void indexTable(String table) {
		try {
			Statement stmt = conn.createStatement();
			addIndex(table, "time_");
			//addIndex(table, "depth");
			//addIndex(table, "lon");
			//addIndex(table, "lat");
			addIndex(table, "duration");
			addIndex(table, "source");
			addIndex(table, "destination");
			// addSpatialIndex(table, "geom");
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean checkIndex(String indexName) {
		try {
			Statement stmt = conn.createStatement();
			stmt.execute("SELECT * from pg_indexes WHERE indexname = '"
					+ indexName + "'");
			ResultSet rs = stmt.getResultSet();
			while (rs.next()) {
				String dbIndexName = rs.getString("indexname");
				if (indexName.equalsIgnoreCase(dbIndexName)) {
					return true;
				}
			}
			return false;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void addIndex(String table, String field) {

		String field2 = field;
		if (field2.endsWith("_")) {
			field2 = field2.substring(0, field2.length() - 1);
		}

		try {
			Statement stmt = conn.createStatement();
			if (checkIndex(table + "_" + field2)) {

				if (overwrite) {
					stmt.execute("DROP INDEX " + table + "_" + field2);
				} else {
					System.out.println(table + "_" + field2
							+ " exists.  Skipping.");
					return;
				}
			}
			String s = "CREATE INDEX " + table + "_" + field2 + " ON " + table
					+ "(" + field + ");";
			long t1 = System.currentTimeMillis();
			stmt.execute(s);
			long t2 = System.currentTimeMillis();
			System.out.printf("\t%-50s%-30s(%-15s)%n",table + "_" + field2 + " created.",new Date(System.currentTimeMillis()), TimeConvert.millisToString(t2 - t1));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void addSpatialIndex(String table, String field) {

		String field2 = field;
		if (field2.endsWith("_")) {
			field2 = field2.substring(0, field2.length() - 1);
		}

		try {
			Statement stmt = conn.createStatement();
			if (checkIndex(table + "_" + field2)) {

				if (overwrite) {
					stmt.execute("DROP INDEX " + table + "_" + field2);
				} else {
					System.out.println(table + "_" + field2
							+ " exists.  Skipping.");
					return;
				}
			}
			String s = "CREATE INDEX " + table + "_" + field2 + " ON " + table
					+ " USING GIST(" + field + ");";
			long t1 = System.currentTimeMillis();
			stmt.execute(s);
			long t2 = System.currentTimeMillis();
			;
			System.out.println("\t" + table + "_" + field2 + " created.\t("
					+ TimeConvert.millisToString(t2 - t1) + ")");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void setConnection(Connection conn){
		this.conn=conn;
	}
}
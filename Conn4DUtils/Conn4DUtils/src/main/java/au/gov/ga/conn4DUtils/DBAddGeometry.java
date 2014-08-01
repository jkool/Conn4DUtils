package au.gov.ga.conn4DUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DBAddGeometry extends DBOperation {

	private boolean overwrite = false;
	private String fieldName = "geom";

	public void addGeom(Date fromDate, Date toDate, int interval,
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
					
					if(tableExists(table) && !fieldExists(table,fieldName)){
						String sql = "SELECT AddGeometryColumn ('public','"+table+"','geom',4326,'POINT',3)";
						stmt.execute(sql);
						System.out.println("Geometry field added to " + table);
					}
				}
			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean checkIndex(String indexName) {
		try {
			Statement stmt = conn.createStatement();
			stmt.execute("SELECT * from pg_indexes WHERE indexname = '"+indexName+"'");
			ResultSet rs = stmt.getResultSet();
			while (rs.next()) {
				String dbIndexName = rs.getString("indexname");
				if(indexName.equalsIgnoreCase(dbIndexName)){
					return true;
				}
			}
			return false;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static void main(String[] args) {
		DBAddGeometry dbi = new DBAddGeometry();
		dbi.connect();
		DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
		// dbi.indexTables(0, 12, 0, 30);
		try {
			dbi.addGeom(df.parse("2009/5/1"), df.parse("2009/9/29"), 30,
					"Days", 0, 30);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		try {
			dbi.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("\nComplete.");
	}
}
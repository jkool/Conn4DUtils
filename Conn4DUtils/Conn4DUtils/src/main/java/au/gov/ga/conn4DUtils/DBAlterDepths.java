package au.gov.ga.conn4DUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBAlterDepths extends DBProcessTableSet {

	public static String tag = "cmr";

	public static void main(String[] args) {
		DBAlterDepths ddp = new DBAlterDepths();
		ddp.connect();
		ddp.batchProcess("^sw_.*d\\d{1,2}$");//.*d\\d{1,2}$");
		ddp.close();
		System.out.println("Complete");
	}

	public void process(String name) throws SQLException {

		Pattern pattern = Pattern.compile("d\\d{1,2}$");
		Matcher matcher = pattern.matcher(name);
		matcher.find();
		int idx = Integer.parseInt(matcher.group().substring(1));
		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);
		stmt.execute("SET work_mem='4GB'");
		ResultSet rs = stmt.executeQuery("SELECT reldepth FROM " + name
				+ " LIMIT 1");
		if(!rs.next()){
			System.out.println("WARNING!!!  EMPTY TABLE!!!" + name);
			return;
		}
		int db = rs.getInt(1);
		if (db != idx) {
			System.out.println("\tAltering depth values... \t\t"
					+ df.format(new Date(System.currentTimeMillis())));

			stmt.execute("CREATE TABLE "+name + "_temp AS SELECT source, "+idx+" reldepth, id, time_, duration, depth, lon, lat, distance, status, destination, nodata, rel_start, geom FROM " + name);
			stmt.execute("DROP TABLE " + name + " CASCADE");
			stmt.execute("ALTER TABLE " + name + "_temp RENAME TO " + name);
			//stmt.execute("ALTER TABLE " + name + " ADD COLUMN key serial");
			DBCleanTables dbc = new DBCleanTables();
			dbc.connect();
			dbc.process(name);

			long stop = System.currentTimeMillis();
			System.out.println(">> " + name + " complete.  Elapsed time: "
					+ TimeConvert.millisToString(stop - start));
		}
		else{System.out.println("Skipping");}
	}
}

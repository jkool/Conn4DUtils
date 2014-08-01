package au.gov.ga.conn4DUtils;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBMergeMatrix extends DBProcessTableSet {

	private static String tag = "cmr";
	private String mrgName = "nw_" + tag + "_nmtx2";
	private SimpleDateFormat format = new SimpleDateFormat("yyyy_MM_dd");
	private SimpleDateFormat df2 = new SimpleDateFormat("yyyy_MM_dd hh:mm:ss");
	private SimpleDateFormat df3 = new SimpleDateFormat("yyyy-MM-dd");
	private PreparedStatement ps;
	private Date maxdate;
	private int maxdepth;

	public static void main(String[] args) {
		DBMergeMatrix dmx = new DBMergeMatrix();
		dmx.connect();
		try {
			dmx.setup();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		dmx.batchProcessViews("nw_201[0-2].*" + tag + "_mtx$");
		dmx.close();
		System.out.println("Complete");
	}

	public void setup() throws SQLException {
		stmt = conn.createStatement();
		
		//stmt.execute("DROP TABLE IF EXISTS " + mrgName);
		
		if (!tableExists(mrgName)) {
			stmt.execute("CREATE TABLE "
					+ mrgName
					+ "("
					+ "RELDATE DATE, DEPTH INTEGER, SOURCE VARCHAR(25), DEST VARCHAR(25), N BIGINT,TOTAL BIGINT, p REAL)");
			maxdate = new Date(Long.MIN_VALUE);
			maxdepth = Integer.MIN_VALUE;
		}
		else{
			ResultSet rsmx = stmt.executeQuery("SELECT MAX(reldate),MAX(depth) FROM " + mrgName + " GROUP BY reldate ORDER BY reldate DESC");
			rsmx.next();
			maxdate = rsmx.getDate(1);
			maxdepth = rsmx.getInt(2);
		}
		
		ps = conn.prepareStatement("INSERT INTO " + mrgName
				+ " VALUES (?,?,?,?,?,?,?)");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	public void process(String name) throws SQLException {

		 if (tableExists(name + "_"+tag+"_cx")) {
		 System.out.println(name + " has been processed.  Skipping.");
		 return;
		 }

		Matcher m = Pattern.compile("[0-9]{4}_[0-9]{2}_[0-9]{2}").matcher(name);
		m.find();
		String s = m.group(0);
		
		long date = 0;
		try {
			date = format.parse(s).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		int depth = Integer.parseInt(name.substring(name.indexOf("d") + 1,
				name.indexOf("_" + tag + "_mtx")));
		
		String sel = "SELECT * FROM " + mrgName + " WHERE reldate='" + df3.format(new Date(date)) +"' AND depth = " + depth;
		ResultSet rss = stmt.executeQuery(sel);

		if(rss.next()){
			System.out.println(name + " has already been processed.  Skipping.");
			return;
		}
		
		//if(new Date(date).before(maxdate)){
		//		System.out.println(name + " has already been processed.  Skipping.");
		//		return;
		//}
		
		//if(new Date(date).toString().equals(maxdate.toString()) && depth<=maxdepth){
		//	System.out.println(name + " has already been processed.  Skipping.");
		//	return;
		//}
		
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);

		stmt.execute("SET work_mem='4GB'");
		ResultSet rs = stmt.executeQuery("SELECT * FROM " + name);

		while (rs.next()) {
			ps.setDate(1, new Date(date));
			ps.setInt(2, depth);
			ps.setString(3, rs.getString(1));
			ps.setString(4, rs.getString(2));
			ps.setLong(5, rs.getLong(3));
			ps.setLong(6, rs.getLong(4));
			ps.setFloat(7, rs.getFloat(5));
			ps.execute();
		}

		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start) + "\t(" + df2.format(new Date(System.currentTimeMillis())) +")");
	}
}

package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBLinkTables extends DBProcessTableSet {

	public boolean overwrite = false;

	public static void main(String[] args) {
		DBLinkTables dln = new DBLinkTables();
		dln.connect();
		// ddt.batchProcess("^nw_2.*d(2[1-9]|3[0-1])$");
		//dct.batchProcess("^nw_.*d\\d{1,2}$");
		//dct.batchProcess("^sw_2012.*");
		dln.batchProcess("^nw_2009.*");
		//dct.batchProcess("^sw_2010_(0[7-9]|1[0-2]).*");
		//dct.batchProcess("^sw_2010_07_25d([2-9]|2[0-9]|3[0-1])$");
		dln.close();
		System.out.println("Complete");
	}

	public void process(String name) throws SQLException {

		stmt = conn.createStatement();
		long start = System.currentTimeMillis();

		System.out.println("\n>> Working on " + name + "\t" + "("
				+ new java.util.Date(start).toString() + ")");

		Pattern p = Pattern.compile("d\\d{1,2}");
		Matcher m = p.matcher(name);
		
		if(!m.find()){
			return;
		}
		
		int position = m.start()+1;
		int dp = Integer.parseInt(name.substring(position, name.length()));
		
		try{stmt.execute("ALTER TABLE " + name + " ADD CONSTRAINT "+name+"_chk_depth CHECK (reldepth=" + dp+"::numeric)");}
		catch (SQLException e){}
		
		try{stmt.execute("ALTER TABLE " + name + " INHERIT " + name.substring(0,position));}
		catch (SQLException e){}
	}
}

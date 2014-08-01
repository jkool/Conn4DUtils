package au.gov.ga.conn4DUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBListEmptyTables extends DBProcessTableSet {

	public static void main(String[] args) {
		DBListEmptyTables dle = new DBListEmptyTables();
		dle.connect();
		dle.batchProcess("^nw_.*$");
		System.out.println("Complete");
	}
	
	public void process(String table) throws SQLException {
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " LIMIT 1");
		if(!rs.isBeforeFirst()){
			System.out.println(table);
		}
	}

}

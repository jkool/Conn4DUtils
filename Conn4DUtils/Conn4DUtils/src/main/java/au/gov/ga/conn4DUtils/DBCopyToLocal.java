package au.gov.ga.conn4DUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBCopyToLocal extends DBProcessTableSet {

	public String dir = "C:/Temp/";
	
	public static void main(String[] args) {
		DBCopyToLocal dbm = new DBCopyToLocal();
		dbm.connect();
		dbm.batchProcess("t2011.*d0$");
		dbm.close();
		System.out.println("Complete");
	}

	public void process(String name) throws SQLException {

		File f = new File(dir+name+".csv"); 
		
		if (f.exists()) {
			System.out.println(name + " has been processed.  Skipping.");
			return;
		}

		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		System.out.println("\n>> Working on " + name);
		stmt.execute("SET work_mem='4GB'");

		DatabaseMetaData meta = conn.getMetaData();
		ResultSet rs = meta.getColumns(null, "public", name, "%");
		StringBuffer sb = new StringBuffer();

		int ncols = 0;
		
		while (rs.next()){
			sb.append(rs.getString(4) + ",");
			ncols++;
		}
		
		sb.setLength(sb.length()-1);
		sb.append("\n");
		
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(f))) {
			bw.write(sb.toString());
			rs.close();
			Statement stmt2 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			stmt2.setFetchSize(10);
			rs = stmt2.executeQuery("SELECT * FROM " + name);
			StringBuffer sb2 = new StringBuffer();
			while(rs.next()){
				for(int i = 0; i < ncols; i++){
					sb2.append(rs.getObject(i).toString() + ",");
				}
				sb.setLength(sb.length()-1);
				sb.append("\n");
			}
			bw.write(sb2.toString());
			bw.flush();
			bw.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
}

package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

import org.postgresql.util.PSQLException;

public class DBDropViews extends DBProcessTableSet{

	public String tag = "cmr";
	
	public static void main(String[] args) {
		DBDropViews ddi = new DBDropViews();
		ddi.connect();
		ddi.batchProcessViews("sw_2010_0[7-8].*_cmr_.*");
		ddi.close();
		System.out.println("Complete");
	}
	
	public void process(String name) throws SQLException {

		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);
		System.out.println("\tDropping views...\t\t(" + new Date(System.currentTimeMillis())+")");
		//if(!tableExists(name)){
		//  System.out.println("View " + name + "does not exist.  Skipping.");
		//  return;
	    //}
		try{
		stmt.execute("DROP VIEW " + name + " CASCADE");}
		catch(PSQLException psq){
			return;
		}
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
}

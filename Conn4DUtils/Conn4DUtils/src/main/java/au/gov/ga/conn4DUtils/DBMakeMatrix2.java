package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

public class DBMakeMatrix2 extends DBProcessTableSet {

	private String tag = "cmr";
	private String id = "mpa_name";
	private String whereClause = "";

	public static void main(String[] args) {
		DBMakeMatrix2 dmm2 = new DBMakeMatrix2();
		dmm2.connect();
		dmm2.batchProcess("nw.*d\\d{1,2}$");
		// did.batchProcess("t2009_01_01d1$");
		dmm2.close();
		System.out.println("Complete");
	}

	public void process(String name) throws SQLException {

		 if (tableExists(name + "_" + tag + "_mtx")) {
		 System.out.println(name + " has been processed.  Skipping.");
		 return;
		 }

		String startName = name + "_" + tag + "_start";
		String destName = name + "_" + tag + "_dst";
		String pathName = name + "_" + tag + "_path";
		String mtxName = name + "_" + tag + "_mtx";

		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);
		stmt.execute("SET work_mem='4GB'");

		stmt.execute("DROP VIEW IF EXISTS " + name + "_" + tag + "_mtx");
		System.out.println("\tGenerating matrix...\t("
				+ new Date(System.currentTimeMillis()) + ")");
		/*stmt.execute("CREATE VIEW "
				+ mtxName
				+ " AS SELECT t6.source,t6.dest,t6.n,t6.total,t6.p FROM (SELECT t5.*,srt.sort_id did FROM (SELECT t4.source,t3.dest,t3.n,t4.total,(t3.n::real/t4.total) p FROM (SELECT t2.mpa_name source, t1.mpa_name dest, COUNT(*) n FROM "
				+ destName
				+ " t1 INNER JOIN "
				+ startName
				+ " t2 ON t1.relid=t2.relid "
				+ whereClause
				+ "GROUP BY t1.mpa_name,t2.mpa_name) t3 INNER JOIN (SELECT mpa_name source,COUNT(*) total FROM "
				+ pathName
				+ " GROUP BY mpa_name) t4 ON t3.source=t4.source ORDER BY t4.source,t3.dest) t5 INNER JOIN "
				+ tag + "_nw_sort srt ON srt." + tag
				+ "_name=t5.dest) t6 INNER JOIN " + tag
				+ "_nw_sort srt ON srt." + tag
				+ "_name=t6.source ORDER BY srt.sort_id,t6.did");*/
		
		//String id = "sort";
		
		stmt.execute("CREATE VIEW "
				+ mtxName
				+ " AS SELECT t5.source,t5.dest,t5.n,t5.total,t5.p FROM (SELECT t4.source,t3.dest,t3.n,t4.total,(t3.n::real/t4.total) p FROM (SELECT t2."+id+" source, t1."+id+" dest, COUNT(*) n FROM "
				+ destName
				+ " t1 INNER JOIN "
				+ startName
				+ " t2 ON t1.relid=t2.relid "
				+ whereClause
				+ "GROUP BY t1."+id+",t2."+id+") t3 INNER JOIN (SELECT "+id+" source,COUNT(*) total FROM "
				+ pathName
				+ " GROUP BY "+id+") t4 ON t3.source=t4.source ORDER BY t4.source,t3.dest) t5 ORDER BY t5.source,t5.dest");		
		
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
}

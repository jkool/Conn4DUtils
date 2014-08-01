package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

public class DBMakeMatrix extends DBProcessTableSet {

	private String tag = "cmr";
	private String whereClause = "WHERE DURATION > 5";

	public static void main(String[] args) {
		DBMakeMatrix did = new DBMakeMatrix();
		did.connect();
		did.batchProcess("n_.*d\\d{1,2}$");
		// did.batchProcess("t2009_01_01d1$");
		did.close();
		System.out.println("Complete");
	}

	public void process(String name) throws SQLException {

		String startName = name + "_" + tag + "_start";
		String destName = name + "_" + tag + "_dst";
		String pathName = name + "_" + tag + "_path";
		String mtxName = name + "_" + tag + "_rmtx";

		if (tableExists(mtxName)) {
			System.out.println(name + " has been processed.  Skipping.");
			return;
		}

		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);
		stmt.execute("SET work_mem='4GB'");
		// System.out.println("\tGenerating indexes...\t("
		// + new Date(System.currentTimeMillis()) + ")");
		// if (!indexExists(startName, startName + "_relid")) {
		// stmt.execute("CREATE INDEX " + startName + "_relid ON " + startName
		// + "(relid)");
		// }
		// if (!indexExists(destName, destName + "_relid")) {
		// stmt.execute("CREATE INDEX " + destName + "_relid ON " + destName
		// + "(relid)");
		// }
		/*
		 * stmt.execute("DROP TABLE IF EXISTS " + name + "_" + tag + "_mtx_5d");
		 * System.out.println("\tGenerating matrix...\t(" + new
		 * Date(System.currentTimeMillis()) + ")"); stmt.execute("CREATE VIEW "
		 * + mtxName +
		 * " AS SELECT * FROM (WITH t1 AS (SELECT MAX(mpa_name) source,relid FROM "
		 * + startName +
		 * " GROUP BY relid) SELECT t3.*,t4.total,(t3.count::real/t4.total) AS p FROM (SELECT source,dest,COUNT(t1.relid) FROM t1 INNER JOIN (SELECT MAX(mpa_name) dest,relid FROM "
		 * + destName + " " + whereClause +
		 * " GROUP BY relid) t2 ON t1.relid=t2.relid GROUP BY source,dest) t3 INNER JOIN (SELECT source,COUNT(relid) total FROM t1 GROUP BY source) t4 ON t3.source=t4.source) t5"
		 * );
		 */
		stmt.execute("DROP VIEW IF EXISTS " + mtxName);
		System.out.println("\tGenerating matrix...\t("
				+ new Date(System.currentTimeMillis()) + ")");
		stmt.execute("CREATE VIEW "
				+ mtxName
				+ " AS SELECT t6.source,t6.dest,t6.n,t6.total,t6.p FROM (SELECT t5.*,srt.sort_id did FROM (SELECT t4.source,t3.dest,t3.n,t4.total,(t3.n::real/t4.total) p FROM (SELECT t2.mpa_name source, t1.mpa_name dest, COUNT(DISTINCT(t1.relid)) n FROM "
				+ destName
				+ " t1 INNER JOIN "
				+ startName
				+ " t2 ON t1.relid=t2.relid "
				+ whereClause
				+ " GROUP BY t1.mpa_name,t2.mpa_name) t3 INNER JOIN (SELECT mpa_name source,COUNT(DISTINCT(relid)) total FROM "
				+ pathName
				+ " GROUP BY mpa_name) t4 ON t3.source=t4.source ORDER BY t4.source,t3.dest) t5 INNER JOIN "
				+ tag + "_nw_sort srt ON srt." + tag
				+ "_name=t5.dest) t6 INNER JOIN " + tag
				+ "_nw_sort srt ON srt." + tag
				+ "_name=t6.source ORDER BY srt.sort_id,t6.did");
		// stmt.execute("CREATE TABLE " + name + "_" + tag +
		// "cx2 AS SELECT t1.MPA_NAME source_cmr, t2.MPA_NAME dest_cmr, COUNT(*) FROM "
		// + name + "_" + tag +"_start t1 INNER JOIN " + name + "_" + tag +
		// "_dst t2 ON (t1.relid=t2.relid) GROUP BY t1.mpa_name,t2.mpa_name");
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
}

package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.util.Date;

public class DBMakeReversePath extends DBProcessTableSet {

	public String tag = "canyons";

	public static void main(String[] args) {
		DBMakeReversePath dst = new DBMakeReversePath();
		dst.connect();
		dst.batchProcess("sw_20.*d\\d{1,2}$");// .*d\\d{1,2}$");
		//dst.batchProcess("t2009_01_01d0$");// .*d\\d{1,2}$");
		dst.close();
		System.out.println("Complete");
	}

	public void process(String name) throws SQLException {

		if (tableExists(name + "_" + tag + "_rev")) {
			System.out.println(name + " has been processed.  Skipping.");
			return;
		}

		stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		System.out.println(">> Working on " + name);
		stmt.execute("DROP VIEW IF EXISTS " + name + "_" + tag + "_rev");
		//System.out.println("\tCreating indexes...\t\t("
		//		+ new Date(System.currentTimeMillis()) + ")");
		//if (!indexExists(name + "_" + tag + "_dst", name + "_" + tag + "_dst_" + "duration")) {
		//	stmt.execute("CREATE INDEX " + name + "_" + tag
		//			+ "_dst_duration ON " + name + "_" + tag + "_dst(duration)");
		//}
		//if (!indexExists(name + "_" + tag + "_dst", name + "_" + tag + "_dst_" +  "relid")) {
		//	stmt.execute("CREATE INDEX " + name + "_" + tag + "_dst_relid ON "
		//			+ name + "_" + tag + "_dst(relid)");
		//}
		System.out.println("\tCreating the table...\t\t("
				+ new Date(System.currentTimeMillis()) + ")");
		stmt.execute("CREATE VIEW "
				+ name
				+ "_"
				+ tag
				//+ "_rev AS SELECT t1.*,t2.Network,t2.MPA_NAME,t2.ZONE,t2.IUCN,t2.Label,t2.cmr_status,t2.Area_km2,t2.Legend FROM "
				+ "_rev AS SELECT t1.*,t2.sort FROM "
				+ name
				//+ " t1 INNER JOIN (SELECT relid,MAX(duration) duration,MAX(network) network, MAX(mpa_name) mpa_name, MAX(\"zone\") \"zone\", MAX(iucn) iucn, MAX(\"label\") \"label\", MAX(cmr_status) cmr_status, MAX(area_km2) area_km2, MAX(legend) legend FROM "
				+ " t1 INNER JOIN (SELECT relid, MAX(duration) duration, MAX(sort) sort FROM "
				+ name
				+ "_"
				+ tag
				+ "_dst GROUP BY relid) t2 ON t1.relid=t2.relid WHERE t1.duration<t2.duration");
		long stop = System.currentTimeMillis();
		System.out.println(">> " + name + " complete.  Elapsed time: "
				+ TimeConvert.millisToString(stop - start));
	}
}

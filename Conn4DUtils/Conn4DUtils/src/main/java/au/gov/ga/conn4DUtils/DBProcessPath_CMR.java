package au.gov.ga.conn4DUtils;

public class DBProcessPath_CMR extends DBProcessTable {

	private String tag = "cmr";

	public String getSQL(String name) {
		return "CREATE VIEW "
				+ getOutputName(name)
				+ " AS SELECT t1.*,"
				+ "t2.Network,"
				+ "t2.MPA_NAME,"
				+ "t2.ZONE,"
				+ "t2.IUCN,"
				+ "t2.Label,"
				+ "t2.Area_km2,"
				+ "t2.Legend FROM "
				+ name + " t1 INNER JOIN " + name + "_" + tag
				+ "_start t2 ON (t1.relid=t2.relid)";
	}

	public String getOutputName(String name) {
		return name + "_" + tag + "_dst";
	}
}
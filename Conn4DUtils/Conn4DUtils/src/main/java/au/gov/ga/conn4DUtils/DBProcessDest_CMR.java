package au.gov.ga.conn4DUtils;

public class DBProcessDest_CMR extends DBProcessTable {

	private String tag = "cmr";
	private String region = "sw";
	private String destLyr = tag + "_" + region + "_g";

	public String getSQL(String name) {
		return "CREATE VIEW " + getOutputName(name) + " AS SELECT t1.*,"
				+ "t2.sort " + "FROM " + name + " t1 INNER JOIN " + destLyr
				+ " t2 ON ST_Intersects(t1.geom,t2.geom)";
	}

	public String getOutputName(String name) {
		return name + "_" + tag + "_dst";
	}
}
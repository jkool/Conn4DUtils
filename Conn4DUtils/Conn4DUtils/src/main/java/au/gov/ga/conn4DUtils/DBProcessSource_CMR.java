package au.gov.ga.conn4DUtils;

public class DBProcessSource_CMR extends DBProcessTable{

	private String tag = "cmr";
	private String region = "sw";
	private String srcLyr = tag + "_" + region + "_g";
	
	public String getSQL(String name) {
		return "CREATE VIEW "
				+ getOutputName(name)
				+ "AS "
				+ "SELECT t1.*,"
				+ "t2.Network,"
				+ "t2.MPA_NAME,"
				+ "t2.ZONE,"
				+ "t2.IUCN,"
				+ "t2.Label,"
				+ "t2.Area_km2,"
				+ "t2.Legend FROM "
				+ name + " t1 INNER JOIN " + srcLyr
				+ " t2 ON ST_Intersects(t1.geom, t2.geom) WHERE t1.duration=0";
	}
	public String getOutputName(String name){
		return name + "_" + tag + "_start";
	}
}
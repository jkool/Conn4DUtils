package au.gov.ga.conn4DUtils;

public class DBProcessDest_Canyons extends DBProcessTable {

	private String tag = "canyons";
	private String destLyr = tag + "_g";
	
	public static void main(String[] args){
		DBProcessDest_Canyons dbc = new DBProcessDest_Canyons();
		dbc.connect();
		dbc.batchProcess("^sw_2012_01.*");
		dbc.close();
	}
	
	public String getSQL(String name) {	
		return "CREATE VIEW " + getOutputName(name) + " AS SELECT t1.*,"
		+ "t2.sort " + "FROM " + name + " t1 INNER JOIN " + destLyr
		+ " t2 ON ST_Intersects(t1.geom,t2.geom)";
	}
	public String getOutputName(String name){
		return name + "_" + tag + "_dst";
	}
}

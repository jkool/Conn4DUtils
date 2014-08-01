package au.gov.ga.conn4DUtils;

public class DBProcessPath_Canyons extends DBProcessTable {

	private String tag = "canyons";
	
	public static void main(String[] args){
		DBProcessPath_Canyons dbc = new DBProcessPath_Canyons();
		dbc.connect();
		dbc.batchProcess("^sw_2012_(0[2-9]|1[0-2]).*");
		dbc.close();
	}

	public String getSQL(String name) {
		return "CREATE VIEW " + getOutputName(name) + " AS SELECT t1.*,"
				+ "t2.sort FROM " + name + " t1 INNER JOIN " + name + "_" + tag
				+ "_src t2 ON (t1.relid=t2.relid)";
	}

	public String getOutputName(String name) {
		return name + "_" + tag + "_path";
	}
}
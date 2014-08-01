package au.gov.ga.conn4DUtils;

public class DBProcessSource_Canyons extends DBProcessTable{

		private String tag = "canyons";
		private String srcLyr = tag + "_g";
	
		public static void main(String[] args){
			DBProcessSource_Canyons dbc = new DBProcessSource_Canyons();
			dbc.connect();
			dbc.batchProcess("^sw_2009.*d\\d{1,2}$");
			dbc.close();
		}
		
		public String getSQL(String name) {
			return "CREATE VIEW "
					+ getOutputName(name)
					+ " AS SELECT t1.*,"
					+ "t2.sort "
					+ "FROM "
					+ name + " t1 INNER JOIN " + srcLyr
					+ " t2 ON ST_Intersects(t1.geom, t2.geom) WHERE t1.duration=0";
		}
		
		public String getOutputName(String name){
			return name + "_" + tag + "_src";
		}
}

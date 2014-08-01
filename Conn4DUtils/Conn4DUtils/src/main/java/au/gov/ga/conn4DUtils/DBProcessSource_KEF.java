package au.gov.ga.conn4DUtils;

public class DBProcessSource_KEF extends DBProcessTable{

		private String tag = "kef";
		private String region = "sw";
		private String srcLyr = tag + "_" + region + "_g";
		
		public String getSQL(String name) {
			return "CREATE VIEW "
					+ getOutputName(name)
					+ "AS SELECT t1.*,"
					+ "t2.name,"
					+ " t2.region FROM "
					+ name 
					+ " t1 INNER JOIN " 
					+ srcLyr
					+ " t2 ON ST_Intersects(t1.geom, t2.geom) WHERE t1.duration=0";
		}
		
		public String getOutputName(String name){
			return name + "_" + tag + "_src";
		}
}

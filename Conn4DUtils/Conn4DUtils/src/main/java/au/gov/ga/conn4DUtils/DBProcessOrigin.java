package au.gov.ga.conn4DUtils;

public class DBProcessOrigin extends DBProcessTable{
	
	public String getSQL(String name){
		return "CREATE VIEW " + name + "_o AS SELECT * FROM " + name + " WHERE duration=0;";
	}
	
	public String getOutputName(String name){
		return name + "_o";
	}
}

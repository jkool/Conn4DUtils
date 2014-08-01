package au.gov.ga.conn4DUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import com.vividsolutions.jts.geom.GeometryFactory;

public class PGDensity {
	private Connection conn;
	private GeometryFactory gf = new GeometryFactory();
	private String driver = "oracle.jdbc.driver.OracleDriver";
	private String conString = "jdbc:oracle:thin:@sun-db-dev:1521:oradev";
	private Statement stmt;
	
	private double llx = 0;
	private double lly = 0;
	private double cellsize = 1000;
	private double nrows = 1000;
	private double ncols = 1000;
	private double radius = 20000;
	
	public void connect(){
		String username = "nerp";
		char[] password = "olympic2".toCharArray();

			try {
				Class.forName(driver);

				// conn = DriverManager.getConnection(conString, "u83869",
				// "<password here>");

				conn = DriverManager.getConnection(conString, username,
						String.valueOf(password));

				Arrays.fill(password, ' ');
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		
	}
	
	public void process(String tablename){
		for (int i = 0; i < nrows; i++){
			for (int j = 0; j < ncols; j++){
				double px = i*cellsize + (cellsize/2d);
				double py = j*cellsize + (cellsize/2d);
				//String sql = "SELECT COUNT(*) FROM (SELECT * FROM ST_DWithin(ST_Transform()" + ") + ")";
			}
		}
	}
	
	public void close() {
		try {
			stmt.close();
			//ps2.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private boolean tableExists(String name) throws SQLException {
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getTables(null, null, "%", null);
		while (rs.next()) {
			if (rs.getString(3).equalsIgnoreCase(name)) {
				return true;
			}
		}
		return false;
	}
}

package au.gov.ga.conn4DUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;

public class Conn4DDatabaseWriter implements Conn4DWriter{

	private Connection conn;
	private String driver = "oracle.jdbc.driver.OracleDriver";
	private String conString = "jdbc:oracle:thin:@sun-db-dev:1521:oradev";
	private Statement stmt;
	private boolean overwrite = true;
	private PreparedStatement ps1;
	private String source = "";
	private double reldepth = -1;
	private long ID = -1;
	private long time;
	private double duration = Double.NaN;
	private double depth = Double.NaN;
	private double lon = Double.NaN;
	private double lat = Double.NaN;
	private double distance = Double.NaN;
	private String status = "";
	private String destination = "";
	private boolean nodata = false;

	public void connect() {
		String username = "nerp";
		char[] password = "olympic2".toCharArray();

		/*
		 * Console console = System.console();
		 * 
		 * if (console != null) { username =
		 * console.readLine("Enter USERNAME: "); password =
		 * console.readPassword("Enter PASSWORD: "); } else { // Using a JPanel
		 * as the message for the JOptionPane JPanel userPanel = new JPanel();
		 * userPanel.setLayout(new GridLayout(2, 2));
		 * 
		 * // Labels for the textfield components JLabel usernameLbl = new
		 * JLabel("Username:"); JLabel passwordLbl = new JLabel("Password:");
		 * 
		 * JTextField usernameJ = new JTextField(); JPasswordField passwordFld =
		 * new JPasswordField();
		 * 
		 * // Add the components to the JPanel userPanel.add(usernameLbl);
		 * userPanel.add(usernameJ); userPanel.add(passwordLbl);
		 * userPanel.add(passwordFld);
		 * 
		 * // As the JOptionPane accepts an object as the message // it allows
		 * us to use any component we like - in this case // a JPanel containing
		 * the dialog components we want
		 * 
		 * JOptionPane.showConfirmDialog(null, userPanel,
		 * "Login to Oracle Database:", JOptionPane.OK_CANCEL_OPTION,
		 * JOptionPane.PLAIN_MESSAGE);
		 * 
		 * username = usernameJ.getText(); password = passwordFld.getPassword();
		 * }
		 */

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

	public void openTable(String tableName) {

		String sql;

		try {

			if (overwrite) {
				if (tableExists(tableName)) {
					stmt = conn.createStatement();
					stmt.executeUpdate("DROP TABLE " + tableName);
					stmt.close();
					conn.commit();
				}

				stmt = conn.createStatement();
				sql = "CREATE TABLE " + tableName + "(" +

				"SOURCE VARCHAR(25), " + "RELDEPTH NUMBER, "
						+ "ID NUMBER(19) NOT NULL, " + "TIME_ TIMESTAMP, "
						+ "DURATION NUMBER, " + "DISTANCE NUMBER, "
						+ "STATUS VARCHAR(3), " + "DESTINATION VARCHAR(25), "
						+ "NODATA NUMBER(1))";

				stmt.execute(sql);

				sql = "SELECT AddGeometryColumn ('public'," + tableName
						+ ",'geom',4326,'POINT',3)";
				stmt.execute(sql);

				stmt.close();
				conn.commit();
				System.out.println("Trajectory table setup complete.");
			}

			sql = "INSERT INTO "
					+ tableName
					+ "("
					+ "SOURCE, "
					+ "RELDEPTH, "
					+ "ID, "
					+ "TIME_, "
					+ "DURATION, "
					+ "DISTANCE, "
					+ "STATUS, "
					+ "DESTINATION, "
					+ "NODATA, "
					+ "geom) "
					+ "VALUES(?,?,?,?,?,?,?,?,?,ST_SetSRID(ST_MakePoint(?, ?, ?), 4326));";

			ps1 = conn.prepareStatement(sql);

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

	public void closeTable() {
		try {
			ps1.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void closeConnection() {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void close(){
		closeTable();
		closeConnection();
	}
	
	public void executeBatch() throws SQLException{
		ps1.executeBatch();
	}

	public void writeToBatch() throws SQLException {
		ps1.setString(1, source);
		ps1.setInt(2, (int) reldepth);
		ps1.setLong(3, ID);
		ps1.setTimestamp(4, new Timestamp(time));
		ps1.setDouble(5, duration);
		ps1.setDouble(6, distance);
		ps1.setString(7, status);
		ps1.setString(8, destination);
		ps1.setBoolean(9, nodata);
		ps1.setDouble(10, lon);
		ps1.setDouble(11, lat);
		ps1.setDouble(12, depth);
		ps1.addBatch();
	}
	
	public void write() {
		try {
			ps1.setString(1, source);
			ps1.setInt(2, (int) reldepth);
			ps1.setLong(3, ID);
			ps1.setTimestamp(4, new Timestamp(time));
			ps1.setDouble(5, duration);
			ps1.setDouble(6, distance);
			ps1.setString(7, status);
			ps1.setString(8, destination);
			ps1.setBoolean(9, nodata);
			ps1.setDouble(10, lon);
			ps1.setDouble(11, lat);
			ps1.setDouble(12, depth);
			ps1.addBatch();
			ps1.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public double getReldepth() {
		return reldepth;
	}

	public void setReldepth(double reldepth) {
		this.reldepth = reldepth;
	}

	public long getID() {
		return ID;
	}

	public void setID(long iD) {
		ID = iD;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public double getDuration() {
		return duration;
	}

	public void setDuration(double duration) {
		this.duration = duration;
	}

	public double getDepth() {
		return depth;
	}

	public void setDepth(double depth) {
		this.depth = depth;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public boolean isNodata() {
		return nodata;
	}

	public void setNodata(boolean nodata) {
		this.nodata = nodata;
	}	
}
package au.gov.ga.conn4DUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PGUtils {

	private Connection conn;
	private String driver = "org.postgresql.Driver";
	private String conString = "jdbc:postgresql://localhost:5432/dh8_connectivity";
	private Statement stmt;
	private DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	private String durationUnits = "Days";
	private String timeUnits = "Date";
	private boolean overwrite = true;
	private PreparedStatement ps1, ps2;
	private static ArrayList<String> depthvals = new ArrayList<String>();
	private boolean skip = true;

	// private boolean negCoord;

	public PGUtils() {

		depthvals.add("0-10"); // 1
		depthvals.add("10-20"); // 2
		depthvals.add("20-30"); // 3
		depthvals.add("30-50"); // 4
		depthvals.add("50-75"); // 5
		depthvals.add("75-100"); // 6
		depthvals.add("100-125"); // 7
		depthvals.add("125-150"); // 8
		depthvals.add("150-200"); // 9
		depthvals.add("200-250"); // 10
		depthvals.add("250-300"); // 11
		depthvals.add("300-400"); // 12
		depthvals.add("400-500"); // 13
		depthvals.add("500-600"); // 14
		depthvals.add("600-700"); // 15
		depthvals.add("700-800"); // 16
		depthvals.add("800-900"); // 17
		depthvals.add("900-1000"); // 18
		depthvals.add("1000-1100"); // 19
		depthvals.add("1100-1200"); // 20
		depthvals.add("1200-1300"); // 21
		depthvals.add("1400-1500"); // 22
		depthvals.add("1500-1750"); // 23
		depthvals.add("1750-2000"); // 24
		depthvals.add("2000-2500"); // 25
		depthvals.add("2500-3000"); // 26
		depthvals.add("3000-3500"); // 27
		depthvals.add("3500-4000"); // 28
		depthvals.add("4000-4500"); // 29
		depthvals.add("4500-5000"); // 30
		depthvals.add("5000-5500"); // 31
	}

	public void connect() {
		String username = "username";
		char[] password = "password".toCharArray();

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
			
			stmt = conn.createStatement();

			Arrays.fill(password, ' ');
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			ps1.close();
			ps2.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private boolean tableExists(String name) throws SQLException {
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getTables(null, null, name, null);
		if (rs.next()) {
			return true;
		}
		return false;
	}

	private boolean fieldExists(String tableName, String fieldName)
			throws SQLException {
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getColumns(null, null, tableName, fieldName);
		if (rs.next()) {
			return true;
		}
		return false;
	}

	public String getTimeUnits() {
		return timeUnits;
	}

	public void setTimeUnits(String timeUnits) {
		this.timeUnits = timeUnits;
	}

	public String getDurationUnits() {
		return durationUnits;
	}

	public void setDurationUnits(String durationUnits) {
		this.durationUnits = durationUnits;
	}

	public ResultSet getTablesAsResultSet() {
		try {
			DatabaseMetaData md = conn.getMetaData();
			return md.getTables(null, null, "%", null);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public List<String> getTableNames(){
		List<String> tableNames = new ArrayList<String>();
		try {
			ResultSet rs = getTablesAsResultSet();
			while(rs.next()){
				String s = rs.getString("TABLE_TYPE");
				String name = rs.getString("TABLE_NAME");
				if(s!=null && s.equalsIgnoreCase("TABLE") 
				 && !name.equalsIgnoreCase("spatial_ref_sys") && 
				 !name.equalsIgnoreCase("geometry_columns")){
				tableNames.add(name);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return tableNames;
	}
	
	public void makeIndex(String table, String field){
		
	}

	public void makePoints(String table, String field) {
		try {
			if (!fieldExists(table, field)) {
				stmt.executeUpdate("ALTER TABLE " + table + " ADD COLUMN "
						+ field + " geometry;");
			}
			stmt.executeUpdate("UPDATE " + table + " SET " + field
					+ " = ST_SetSRID(ST_MakePoint(LON,LAT,DEPTH),4326);");
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void updateField(String table, String field, double value) {
		try {
			String cmd = "UPDATE " + table + " SET " + field + " = " + value;
			stmt.executeUpdate(cmd);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void createPrimaryKey(String table, String fieldName) {
		try {
			String cmd = "ALTER TABLE " + table + " ADD COLUMN " + fieldName
					+ " serial PRIMARY KEY;";
			stmt.executeUpdate(cmd);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void updateField(String table, String field, String value) {
		try {
			String cmd = "UPDATE " + table + " SET " + field + " = '" + value
					+ "'";
			stmt.executeUpdate(cmd);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void clean() {
		try {
			DatabaseMetaData md = conn.getMetaData();
			ResultSet rs = md.getTables(null, null, "%", null);
			stmt = conn.createStatement();
			while (rs.next()) {
				String name = rs.getString(3);
				if (name.contains("T1564915759")
						|| name.contains("T2003359763")
						|| name.contains("T2078457880")
						|| name.contains("T225294351")
						|| name.contains("T300392468")
						|| name.contains("T375490585")
						|| name.contains("T450588702")
						|| name.contains("T525686819")
						|| name.contains("T600784936")
						|| name.contains("T675883053")
						|| name.contains("T75098117")
						|| name.contains("T826079287")
						|| name.contains("T964130823")) {
					System.out.println(name);
					stmt.executeUpdate("DROP TABLE " + name);
				}
			}
			stmt.close();
			conn.commit();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// if (args.length != 2) {
		// System.out
		// .println("Usage:  TextDir2DB <path location> <table name>");
		// System.exit(0);
		// }
		System.out.println("Starting...");
		PGUtils pg = new PGUtils();
		pg.connect();
		List<String> names = pg.getTableNames();
		int idx = names.indexOf("t2d2");
		names = names.subList(idx, names.size());
		Iterator<String> it = names.iterator();
		
		while(it.hasNext()){
			String nm = it.next();
			System.out.print("Working on " + nm + ".");
			pg.makePoints(nm,"geom");
			System.out.println("\t" + "Complete.");
		}

		// td.clean();
		System.out.println("Complete.");
	}
}

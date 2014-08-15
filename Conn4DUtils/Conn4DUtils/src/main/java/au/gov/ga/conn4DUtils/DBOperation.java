package au.gov.ga.conn4DUtils;

import java.awt.GridLayout;
import java.io.Console;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

public class DBOperation {

	protected Connection conn;
	// private String driver = "oracle.jdbc.driver.OracleDriver";
	// private String conString = "jdbc:oracle:thin:@sun-db-dev:1521:oradev";
	private String driver = "org.postgresql.Driver";
	// private String conString =
	// "jdbc:postgresql://pdb7.anu.edu.au/dh8_connectivity";
	private String conString = "jdbc:postgresql://127.0.0.1/dh8_connectivity";
	private String username = "user";
	private char[] password = "password".toCharArray();

	protected static ArrayList<String> depthvals = new ArrayList<String>();

	public DBOperation() {

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
		depthvals.add("1300-1400"); // 22
		depthvals.add("1400-1500"); // 23
		depthvals.add("1500-1750"); // 24
		depthvals.add("1750-2000"); // 25
		depthvals.add("2000-2500"); // 26
		depthvals.add("2500-3000"); // 27
		depthvals.add("3000-3500"); // 28
		depthvals.add("3500-4000"); // 29
		depthvals.add("4000-4500"); // 30
		depthvals.add("4500-5000"); // 31
		depthvals.add("5000-5500"); // 32
	}

	public void connect() {

		Console console = System.console();
		if (username.isEmpty() || password == null || password.length == 0) {
			if (console != null) {
				username = console.readLine("Enter USERNAME: ");
				password = console.readPassword("Enter PASSWORD: ");

			} else {
				// Using a JPanel as the message for the JOptionPane
				JPanel userPanel = new JPanel();
				userPanel.setLayout(new GridLayout(2, 2));

				// Labels for the textfield components
				JLabel usernameLbl = new JLabel("Username:");
				JLabel passwordLbl = new JLabel("Password:");

				JTextField usernameJ = new JTextField();
				JPasswordField passwordFld = new JPasswordField();

				// Add the components to the JPanel
				userPanel.add(usernameLbl);
				userPanel.add(usernameJ);
				userPanel.add(passwordLbl);
				userPanel.add(passwordFld);

				// As the JOptionPane accepts an object as the message
				// it allows us to use any component we like - in this case
				// a JPanel containing the dialog components we want

				JOptionPane.showConfirmDialog(null, userPanel,
						"Login to Database:", JOptionPane.OK_CANCEL_OPTION,
						JOptionPane.PLAIN_MESSAGE);

				username = usernameJ.getText();
				password = passwordFld.getPassword();
			}
		}

		try {
			Class.forName(driver);

			conn = DriverManager.getConnection(conString, username,
					String.valueOf(password));

			Arrays.fill(password, ' ');

			Statement st = conn.createStatement();
			st.execute("SET work_mem='4GB'");
			st.close();

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	protected boolean tableExists(String name) throws SQLException {
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getTables(null, "public", "%", null);
		while (rs.next()) {
			if (rs.getString(3).equalsIgnoreCase(name)) {
				return true;
			}
		}
		return false;
	}
	
	protected boolean indexExists(String tableName, String fieldName) throws SQLException {
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getIndexInfo(null, null, tableName, false,true);
		while (rs.next()) {
			if (rs.getString("TABLE_NAME").equals(tableName) && rs.getString("INDEX_NAME").equals(fieldName)) {
				return true;
			}
		}
		return false;
	}

	protected boolean fieldExists(String tableName, String fieldName) {
		try {
			DatabaseMetaData md = conn.getMetaData();
			ResultSet rs = md.getColumns(null, null, tableName, fieldName);
			if (rs.next()) {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	protected List<String> listTables() throws SQLException {
		List<String> names = new ArrayList<String>();
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getTables(null, "public", "%",
				new String[] { "TABLE" });
		while (rs.next()) {
			names.add(rs.getString(3));
		}
		return names;
	}
	
	protected List<String> listViews() throws SQLException {
		List<String> names = new ArrayList<String>();
		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs = md.getTables(null, "public", "%",
				new String[] { "VIEW" });
		while (rs.next()) {
			names.add(rs.getString(3));
		}
		return names;
	}

	protected void close() {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getConString() {
		return conString;
	}

	public void setConString(String conString) {
		this.conString = conString;
	}
}

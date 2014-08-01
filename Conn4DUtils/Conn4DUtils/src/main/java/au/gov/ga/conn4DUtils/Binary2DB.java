package au.gov.ga.conn4DUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TimeZone;

import com.vividsolutions.jts.geom.GeometryFactory;

public class Binary2DB {

	File f;
	DataInputStream in;
	DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");
	private Connection conn;
	private GeometryFactory gf = new GeometryFactory();
	private String driver = "oracle.jdbc.driver.OracleDriver";
	private String conString = "jdbc:oracle:thin:@sun-db-dev:1521:oradev";
	private Statement stmt;
	private DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	private String durationUnits = "Days";
	private String timeUnits = "Date";
	private boolean overwrite = true;
	private PreparedStatement ps1;//, ps2;

	public static void main(String[] args){
		Binary2DB b2 = new Binary2DB();
		b2.setFile(new File("G:/Model/Output/Latz/2010-12-01.dat"));
		b2.printFile();
		//b2.convert("G:/Model/Output/Latz/2010-12-01.dat", "G:/Model/Output/Latz/2010-12-01.txt");
		//System.out.println(b2.wasProperlyClosed());
		//System.out.println(b2.getLastLine());
		//b2.printFile();
	}
	
	public Binary2DB(){}
	
	public Binary2DB(File f){
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		setFile(f);
	}
	
	public Binary2DB(String s){
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		setFile(new File(s));
	}
	
	public String getLastLine(){
		try (RandomAccessFile file = new RandomAccessFile(f,"r")) {
			long index,length;
			length = file.length();
			index = length-1;
			int ch = 0;
			
			while(ch!=30 && index>0){
				file.seek(index--);
				ch = (file.read());
			}
			
			if(index==0){return null;}
			
			file.seek(index--);
			ch = (file.read());
			
			while(ch!=30 && index>0){
				file.seek(index--);
				ch = (file.read());
			}
			
			file.seek(index++);
			
			StringBuilder sb = new StringBuilder();
			
			sb.append(in.readUTF());
			sb.append("\t");
			in.readChar();
			sb.append(in.readLong());
			sb.append("\t");
			in.readChar();
			sb.append(df.format(new Date(in.readLong())));
			sb.append("\t");
			in.readChar();
			sb.append(TimeConvert.millisToDays(in.readLong()));
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			String code = in.readUTF();
			sb.append(code);
			if(code=="S"){
				sb.append(in.readUTF());
			}
			in.readChar();
			sb.append(in.readBoolean());

			return sb.toString();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
		return null;
	}
	
	public void connect(){
		String username = "nerp";
		char[] password = "olympic2".toCharArray();

		/*Console console = System.console();

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
					"Login to Oracle Database:", JOptionPane.OK_CANCEL_OPTION,
					JOptionPane.PLAIN_MESSAGE);

			username = usernameJ.getText();
			password = passwordFld.getPassword();
		}*/

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
	
	public void makeTable(String tableName) {

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
						+ "DURATION NUMBER, " + "DEPTH NUMBER, "
						+ "LON NUMBER, " + "LAT NUMBER, " + "DISTANCE NUMBER, "
						+ "STATUS VARCHAR(3), " + "DESTINATION VARCHAR(25), "
						+ "NODATA NUMBER(1), " + "RELEASE VARCHAR(25))";

				stmt.execute(sql);
				
				sql = "SELECT AddGeometryColumn ('public',"+tableName+",'geom',4326,'POINT',3)";
				stmt.execute(sql);
				
				stmt.close();
				conn.commit();
				System.out.println("Trajectory table setup complete.");
			}

			//if (overwrite) {
			//	if (tableExists(tableName + "_SET")) {
			//		stmt = conn.createStatement();
			//		stmt.executeUpdate("DROP TABLE " + tableName + "_SET");
			//		stmt.close();
			//		conn.commit();
			//	}
			//	stmt = conn.createStatement();
			//	sql = "CREATE TABLE " + tableName + "(" +

			//	"SOURCE VARCHAR(25), " + "ID NUMBER(19) NOT NULL, "
			//			+ "TIME_ TIMESTAMP, " + "DURATION NUMBER, "
			//			+ "DEPTH NUMBER, " + "LON NUMBER, " + "LAT NUMBER, "
			//			+ "DISTANCE NUMBER, " + "STATUS VARCHAR(3), "
			//			+ "DESTINATION VARCHAR(25), " + "NODATA NUMBER(1))";

			//	stmt.close();
			//	conn.commit();
			//	System.out.println("Settlement table setup complete.");
			//}

			// May need to add dbname.tablename

			sql = "INSERT INTO " + tableName + "(" +

			//"SOURCE, " + "RELDEPTH, " + "ID, " + "TIME_, " + "DURATION, "
			//		+ "DEPTH, " + "LON, " + "LAT, " + "DISTANCE, " + "STATUS, "
			//		+ "DESTINATION, " + "NODATA, " + "RELEASE) "
			//		+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			
			"SOURCE, " + "RELDEPTH, " + "ID, " + "TIME_, " + "DURATION, "
				  + "DEPTH, " + "LON, " + "LAT, " + "DISTANCE, " + "STATUS, "
				  + "DESTINATION, " + "NODATA, " + "RELEASE, geom) "
				  + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,ST_SetSRID(ST_MakePoint(?, ?, ?), 4326));";

			ps1 = conn.prepareStatement(sql);

			//sql = "INSERT INTO " + tableName + "(" +

			//"ID, " + "TIME_, " + "DURATION, " + "DEPTH, " + "LON, " + "LAT, "
			//		+ "DISTANCE, " + "DESTINATION, " + "NODATA) "
			//		+ "VALUES(?,?,?,?,?,?,?,?,?)";

			//ps2 = conn.prepareStatement(sql);

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public boolean wasProperlyClosed(){
		try (RandomAccessFile file = new RandomAccessFile(f,"r")) {
			long index,length;
			length = file.length();
			index = length-1;
			file.seek(index);
			return (file.read()==28);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
		return false;
	}
	
	public String getLastRelease(){
		StringTokenizer stk = new StringTokenizer(getLastLine());
		return stk.nextToken();
	}
	
	public void printFile(){
		boolean EOF = false;
		
		while (!EOF) {
		    try {
		        System.out.println(read());
		    } catch (EOFException e) {
		        EOF = true;
		    }
		} 
	}
	
	public void batchConvert(String directory){
		
		File dir = new File(directory);

		if (!dir.isDirectory()){
			System.out.println(directory + " is not a directory.  Exiting.");
			System.exit(-1);
		}
		
		for(File f:dir.listFiles()){
			String outName = f.getPath().substring(0, f.getPath().lastIndexOf("."))+".txt";
			convert(f,new File(outName));
		}
		
		System.out.println();
		System.out.println("Conversion of directory " + directory + " is complete.");
	}
	
	public void convert(String inputFile, String outputFile){
		convert(new File(inputFile), new File(outputFile));
	}
	
	public void convert(File inputFile, File outputFile){
		
		try {
			outputFile.createNewFile();
		} catch (IOException e1) {
			System.out.println("Could not create output file " + outputFile);
		}
		
		try (FileWriter bw = new FileWriter(outputFile)){
			setFile(inputFile);
			boolean EOF = false;
			
			bw.write("SOURCE\tID\tTIME_\tDURATION\tDEPTH\tLON\tLAT\tDISTANCE\tSTATUS\tNODATA\n");
			
			while (!EOF){
				try {
				    bw.write(read() + "\n");
				} catch (EOFException e){
					EOF = true;
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Input file " + inputFile + " converted to " + outputFile + ".");
	}
	
	public String read() throws EOFException{
		
		StringBuilder sb = new StringBuilder();
		
		try {
			sb.append(in.readUTF());
			sb.append("\t");
			in.readChar();
			sb.append(in.readLong());
			sb.append("\t");
			in.readChar();
			sb.append(df.format(new Date(in.readLong())));
			sb.append("\t");
			in.readChar();
			sb.append(TimeConvert.millisToDays(in.readLong()));
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			String code = in.readUTF();
			sb.append(code);
			if(code=="S"){
				sb.append(in.readUTF());
			}
			sb.append("\t");
			in.readChar();
			sb.append(in.readBoolean());
			in.readChar();
		} catch (EOFException eof) {
			throw new EOFException();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}
	
	public void setFile(File f){
		try {
			 this.f = f; 
			 in = new DataInputStream(new BufferedInputStream(
					//new GZIPInputStream(new FileInputStream(f))));
					new FileInputStream(f)));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		//} catch (IOException e) {
		//	e.printStackTrace();
		}
	}
	
	public void writeTRJ(String filename, int reldepth, String release) {

		File file = new File(filename);
		BufferedReader br = null;
		String ln = null;

		try {
			br = new BufferedReader(new FileReader(file));
			br.readLine();
			ln = br.readLine();
			while (ln != null) {
				StringTokenizer stk = new StringTokenizer(ln);
				if (stk.countTokens() != 10) {
					System.out
							.println("WARNING: Number of fields must equal 10 (contains "
									+ stk.countTokens()
									+ ").  Skipping "
									+ filename + "...");
					ps1.clearBatch();
					return;
				}
				ps1.setString(
						1,
						file.getName().substring(0,
								file.getName().lastIndexOf("."))); // Source
				ps1.setInt(2, reldepth);
				ps1.setInt(3, Integer.parseInt(stk.nextToken())); // Index
				Timestamp ts = null;
				try {
					String s = stk.nextToken() + " " + stk.nextToken();
					if (s.contains(":")) {
						ts = new Timestamp(df1.parse(s).getTime());
					} else {
						ts = new Timestamp(df2.parse(s).getTime());
					}
				} catch (ParseException e) {
					System.out
							.println("WARNING: Date could not be parsed from "
									+ ln + ".  Continuing");
					ln = br.readLine();
					continue;
				}
				ps1.setTimestamp(4, ts); // Time
				ps1.setDouble(5, Double.parseDouble(stk.nextToken())); // Duration
				double depth = Double.parseDouble(stk.nextToken());
				ps1.setDouble(6, depth); // Depth
				double longitude = Double.parseDouble(stk.nextToken());
				ps1.setDouble(7, longitude); // Longitude
				double latitude = Double.parseDouble(stk.nextToken());
				ps1.setDouble(8, latitude); // Latitude
				ps1.setDouble(9, Double.parseDouble(stk.nextToken())); // Distance
				String status = stk.nextToken();
				if (!status.startsWith("\"S")) {
					ps1.setString(10, status); // Status
					ps1.setString(11, "");
				} else {
					ps1.setString(10, "S");
					ps1.setString(11, status.substring(2, status.length() - 1));
				}
				ps1.setBoolean(12, Boolean.parseBoolean(stk.nextToken())); // NoData
				ps1.setString(13, release);
				ps1.setDouble(14, longitude);
				ps1.setDouble(15, latitude);
				ps1.setDouble(16, depth);
				// ps1.execute();
				ln = br.readLine();
				ps1.addBatch();
			}
			ps1.executeBatch();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException sq) {
			sq.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
		}
		// System.out.println(filename + " processing complete");
	}

	public void close() {
		try {
			in.close();
			ps1.close();
			//ps2.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e){
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

	// public void setNegCoord(boolean negCoord) {
	// this.negCoord = negCoord;
	// }
}

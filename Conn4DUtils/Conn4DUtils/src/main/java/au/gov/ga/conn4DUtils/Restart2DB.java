package au.gov.ga.conn4DUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

public class Restart2DB {

	private File inf;
	private DataInputStream in;
	private String driver = "org.postgresql.Driver";
	private Connection conn;
	private String conString = "jdbc:postgresql://localhost/dh8_connectivity";
	private Statement stmt;
	private PreparedStatement ps1;
	private boolean overwrite = true;
	private int sourcedepth;
	private String lastItem;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Restart2DB rsd = new Restart2DB();
		rsd.connect();
		File file = new File("restartFile");
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String ln = br.readLine();
			while(ln!=null){
				StringTokenizer stk = new StringTokenizer(ln);
				String fname = stk.nextToken();
				rsd.procFile(fname,stk.nextToken());
				ln = br.readLine();
				System.out.println(fname + "processed.  " + rsd.sdf.format(new Date(System.currentTimeMillis())));
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void setup(){
		connect();
	}
	
	public void procFile(String filepath, String restart){
		this.lastItem = restart;
		copyTable(filepath);
	}
	
	public void copyTable(String filepath){
		File f = new File(filepath);
		String name = f.getName();
	    String subname = name.substring(0, name.lastIndexOf("."));
	    makeTable(subname);
	    setInputFile(f);
	    writeFile();
	}
	
	public void reset() {
		setInputFile(inf);
	}

	public void setInputFile(File input) {
		inf = input;
		String name = input.getName();
		sourcedepth = Integer.parseInt(name.substring(name.lastIndexOf("D"), name.lastIndexOf(".")));
		
		try {
			in = new DataInputStream(new BufferedInputStream(
					new GZIPInputStream(new FileInputStream(input))));

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void connect() {
		String username = "username";
		char[] password = "password".toCharArray();
		try {
			Class.forName(driver);

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

	/**
	 * Helper class for writing Strings.
	 */

	private static class DataItem implements Item {
		private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");
		private String SOURCE;
		private long ID;
		private long TIME;
		private long DURATION;
		private double Z;
		private double X;
		private double Y;
		private double DISTANCE;
		private String STATUS;
		private boolean NODATA;

		public DataItem() {
			df.setTimeZone(TimeZone.getTimeZone("UTC"));
		}

		public boolean equals(Object other) {
			if (!(other instanceof DataItem)) {
				return false;
			}
			DataItem item = (DataItem) other;
			if (this.SOURCE == item.SOURCE && this.ID == item.ID
					&& this.TIME == item.TIME && this.DURATION == item.DURATION
					&& this.Z == item.Z && this.X == item.X && this.Y == item.Y
					&& this.DISTANCE == item.DISTANCE
					&& this.STATUS == item.STATUS && this.NODATA == item.NODATA) {
				return true;
			}
			return false;
		}

		public double getDistance() {
			return DISTANCE;
		}

		public long getDuration() {
			return DURATION;
		}

		public long getID() {
			return ID;
		}

		public boolean getNoData() {
			return NODATA;
		}

		public String getSource() {
			return SOURCE;
		}

		public String getStatus() {
			return STATUS;
		}

		public long getTime() {
			return TIME;
		}

		public double getX() {
			return X;
		}

		public double getY() {
			return Y;
		}

		public double getZ() {
			return Z;
		}

		public void setDistance(double distance) {
			this.DISTANCE = distance;
		}

		public void setDuration(long duration) {
			this.DURATION = duration;
		}

		public void setID(long id) {
			this.ID = id;
		}

		public void setNoData(boolean nodata) {
			this.NODATA = nodata;
		}

		public void setSource(String source) {
			this.SOURCE = source;
		}

		public void setStatus(String status) {
			this.STATUS = status;
		}

		public void setTime(long time) {
			this.TIME = time;
		}

		public void setX(double x) {
			this.X = x;
		}

		public void setY(double y) {
			this.Y = y;
		}

		public void setZ(double z) {
			this.Z = z;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(SOURCE);
			sb.append("\t");
			sb.append(ID);
			sb.append("\t");
			sb.append(df.format(new Date(TIME)));
			sb.append("\t");
			sb.append(TimeConvert.millisToDays(DURATION));
			sb.append("\t");
			sb.append(Z);
			sb.append("\t");
			sb.append(X);
			sb.append("\t");
			sb.append(Y);
			sb.append("\t");
			sb.append(DISTANCE);
			sb.append("\t");
			sb.append(STATUS);
			sb.append("\t");
			sb.append(NODATA);
			return sb.toString();
		}

		public void write(DataOutputStream out) throws IOException {
			try {
				out.writeUTF(SOURCE);
				out.writeChar(31);
				out.writeLong(ID);
				out.writeChar(31);
				out.writeLong(TIME);
				out.writeChar(31);
				out.writeLong(DURATION);
				out.writeChar(31);
				out.writeDouble(Z);
				out.writeChar(31);
				out.writeDouble(X);
				out.writeChar(31);
				out.writeDouble(Y);
				out.writeChar(31);
				out.writeDouble(DISTANCE);
				out.writeChar(31);
				out.writeUTF(STATUS);
				out.writeChar(31);
				out.writeBoolean(NODATA);
				out.writeChar(30);

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void setLastItem(String lastItem){
		this.lastItem = lastItem;
	}
	
	/**
	 * Generic Item interface for writing objects.
	 */

	private static interface Item {
		void write(DataOutputStream out) throws IOException;
	}

	public void writeFile() {
		boolean EOF = false;
		int ct = 0;
		int tick = 1000;

		while (!EOF) {
			try {
				DataItem di = read();
				if(di.getSource().equalsIgnoreCase(lastItem)){
					throw new EOFException();
				}
				write(di);
			} catch (EOFException e) {
				EOF = true;
			}
			ct++;
			if(ct%tick==0){
				ct=0;
				System.out.println("\tRecord " + ct + " processed.");
			}
			
		}
	}
	
	public void write(DataItem di){
		
		try {
			ps1.setString(1,di.getSource()); // Source
			ps1.setInt(2, sourcedepth);// SourceDepth????
			ps1.setInt(3, (int) di.getID()); // Index
			ps1.setTimestamp(4, new Timestamp(di.getTime())); // Time
			ps1.setDouble(5, di.getDuration()); // Duration
			ps1.setDouble(6, di.getDistance()); // Distance
			ps1.setString(7, di.getStatus());
			ps1.setBoolean(8, di.getNoData()); // NoData
			ps1.setDouble(9, di.getX());
			ps1.setDouble(10, di.getY());
			ps1.setDouble(11, di.getZ());
			// ps1.execute();
			ps1.addBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public DataItem read() throws EOFException {

		DataItem item = new DataItem();

		try {
			item.setSource(in.readUTF());
			in.readChar();
			item.setID(in.readLong());
			in.readChar();
			item.setTime(in.readLong());
			in.readChar();
			item.setDuration(in.readLong());
			in.readChar();
			item.setZ(in.readDouble());
			in.readChar();
			item.setX(in.readDouble());
			in.readChar();
			item.setY(in.readDouble());
			in.readChar();
			item.setDistance(in.readDouble());
			in.readChar();
			item.setStatus(in.readUTF());
			in.readChar();
			item.setNoData(in.readBoolean());
			in.readChar();
		} catch (EOFException eof) {
			throw new EOFException();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return item;
	}
}

package au.gov.ga.conn4DUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Conn4DTextWriter implements Conn4DWriter {

	DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");
	BufferedWriter bw;
	private boolean overwrite = true;
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
	private String folderName = "";
	private String sp = "\t";
	
	public Conn4DTextWriter(){
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	public void openTable(String tableName) {

		File file = new File(folderName + File.separator + tableName);

		if (!overwrite && file.exists()) {
			System.out
					.println("File "
							+ folderName
							+ "/"
							+ tableName
							+ " already exists, and overwrite is set as false.  Exiting.");
			System.exit(-1);
		}

		try {
			bw = new BufferedWriter(new FileWriter(file));

			bw.write("SOURCE" + sp);
			bw.write("RELDEPTH" + sp);
			bw.write("ID" + sp);
			bw.write("TIME_" + sp);
			bw.write("DURATION" + sp);
			bw.write("DEPTH" + sp);
			bw.write("LAT" + sp);
			bw.write("LON" + sp);
			bw.write("DISTANCE" + sp);
			bw.write("STATUS" + sp);
			bw.write("NODATA" + sp);
			bw.write("\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void write(){
		
		StringBuffer sb = new StringBuffer();
		
		sb.append(source + sp);
		sb.append(reldepth + sp);
		sb.append(ID + sp);
		sb.append(df.format(new Date(time)) + sp);
		sb.append(duration + sp);
		sb.append(depth + sp);
		sb.append(lon + sp);
		sb.append(lat + sp);
		sb.append(distance + sp);
		sb.append(status + destination + sp);
		sb.append(nodata);
		sb.append("\n");
		
		try {
			bw.write(sb.toString());
		} catch (IOException e) {
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

	public void setID(long ID) {
		this.ID = ID;
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
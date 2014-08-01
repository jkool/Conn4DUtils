package au.gov.ga.conn4DUtils;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TimeZone;

public class Conn4DBinaryReader implements Conn4DReader {

	private File f;
	private DataInputStream in;
	private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");
	private String source = "";
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
	
	public Conn4DBinaryReader() {
	}

	public Conn4DBinaryReader(File f) {
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		open(f);
	}

	public Conn4DBinaryReader(String s) {
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		open(new File(s));
	}

	public void close() {
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getLastLine() {
		try (RandomAccessFile file = new RandomAccessFile(f, "r")) {
			long index, length;
			length = file.length();
			index = length - 1;
			int ch = 0;

			while (ch != 30 && index > 0) {
				file.seek(index--);
				ch = (file.read());
			}

			if (index == 0) {
				return null;
			}

			file.seek(index--);
			ch = (file.read());

			while (ch != 30 && index > 0) {
				file.seek(index--);
				ch = (file.read());
			}

			file.seek(index++);
			return readLine();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public boolean wasProperlyClosed() {
		try (RandomAccessFile file = new RandomAccessFile(f, "r")) {
			long index, length;
			length = file.length();
			index = length - 1;
			file.seek(index);
			return (file.read() == 28);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public String getLastRelease() {
		StringTokenizer stk = new StringTokenizer(getLastLine());
		return stk.nextToken();
	}

	public void read() throws EOFException {
		try {
			source = in.readUTF();
			in.readChar();
			ID = in.readLong();
			in.readChar();
			time = in.readLong();
			in.readChar();
			duration = TimeConvert.millisToDays(in.readLong());
			in.readChar();
			depth = in.readDouble();
			in.readChar();
			lat = in.readDouble();
			in.readChar();
			lon = in.readDouble();
			in.readChar();
			distance = in.readDouble();
			in.readChar();
			status = in.readUTF();
			if (status.equals("S")) {
				destination = in.readUTF();
			} else {
				destination = "";
			}
			in.readChar();
			nodata = in.readBoolean();
			in.readChar();

		} catch (EOFException eof) {
			throw new EOFException();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String readLine() throws EOFException {

		read();

		StringBuilder sb = new StringBuilder();

		sb.append(source + "\t");
		sb.append(ID + "\t");
		sb.append(df.format(new Date(time)) + "\t");
		sb.append(duration + "\t");
		sb.append(depth + "\t");
		sb.append(lat + "\t");
		sb.append(lon + "\t");
		sb.append(distance + "\t");
		sb.append(status);
		sb.append(destination + "\t");
		sb.append(nodata + "\n");
		return sb.toString();
	}

	public void open(String s) {
		open(new File(s));
	}

	public void open(File f) {
		try {
			this.f = f;
			in = new DataInputStream(new BufferedInputStream(
					new FileInputStream(f)));

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
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

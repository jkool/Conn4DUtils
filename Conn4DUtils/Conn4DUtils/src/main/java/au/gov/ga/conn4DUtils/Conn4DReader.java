package au.gov.ga.conn4DUtils;

import java.io.EOFException;

public interface Conn4DReader {

	public void close();
	public void read() throws EOFException;
	public String readLine() throws EOFException;
	public String getSource();
	public long getID();
	public long getTime();
	public double getDuration();
	public double getDepth();
	public void setDepth(double depth);
	public double getLon();
	public double getLat();
	public double getDistance();
	public String getStatus();
	public String getDestination();
	public boolean isNodata();
}

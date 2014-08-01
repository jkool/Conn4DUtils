package au.gov.ga.conn4DUtils;

public interface Conn4DWriter {

	public void openTable(String tableName);
	public void close();
	public void write();
	public void setSource(String source);
	public void setReldepth(double reldepth);
	public void setID(long ID);
	public void setTime(long time);
	public void setDuration(double duration);
	public void setDepth(double depth);
	public void setLon(double lon);
	public void setLat(double lat);
	public void setDistance(double distance);
	public void setStatus(String status);
	public void setDestination(String destination);
	public void setNodata(boolean nodata);
}

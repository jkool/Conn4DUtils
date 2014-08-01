package au.gov.ga.conn4DUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;


public class RestartTools {

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
		public boolean equals(Object other){
			if(!(other instanceof DataItem)){
				return false;
			}
			DataItem item = (DataItem)other;
			if(this.SOURCE==item.SOURCE &&
			   this.ID==item.ID &&
			   this.TIME==item.TIME &&
			   this.DURATION==item.DURATION &&
			   this.Z==item.Z &&
			   this.X==item.X &&
			   this.Y==item.Y &&
			   this.DISTANCE==item.DISTANCE&&
			   this.STATUS==item.STATUS &&
			   this.NODATA==item.NODATA){return true;}
			return false;		
		}
		public double getDistance(){
			return DISTANCE;
		}
		public long getDuration(){
			return DURATION;
		}
		public long getID(){
			return ID;
		}
		public boolean getNoData(){
			return NODATA;
		}
		public String getSource(){
			return SOURCE;
		}
		public String getStatus(){
			return STATUS;
		}
		public long getTime(){
			return TIME;
		}
		public double getX(){
			return X;
		}
		public double getY(){
			return Y;
		}
		public double getZ(){
			return Z;
		}
		public void setDistance(double distance){
			this.DISTANCE=distance;
		}
		public void setDuration(long duration){
			this.DURATION=duration;
		}
		public void setID(long id){
			this.ID=id;
		}
		public void setNoData(boolean nodata){
			this.NODATA=nodata;
		}
		public void setSource(String source){
			this.SOURCE=source;
		}
		public void setStatus(String status){
			this.STATUS=status;
		}
		public void setTime(long time){
			this.TIME=time;
		}
		public void setX(double x){
			this.X=x;
		}
		public void setY(double y){
			this.Y=y;
		}
		public void setZ(double z){
			this.Z=z;
		}
		
	 
		public String toString(){
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
	/**
	 * Generic Item interface for writing objects.
	 */
	
	private static interface Item {
		void write(DataOutputStream out) throws IOException;
	}
	
	private DataInputStream in;
	private DataOutputStream out;
	private File inf;
	private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");
	private DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private ShapefileReader sr;
	private String source;
	
	private int[] mindepths = new int[]{0,10,20,30,50,75,100,125,150,200,250,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1750,2000,2500,3000,3500,4000,4500,5000};
	private int[] maxdepths = new int[]{10,20,30,50,75,100,125,150,200,250,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1750,2000,2500,3000,3500,4000,4500,5000,5500};
	private Date[] releaseDates = new Date[]{};
	private String prms = "-Xss1024k -Xmx8g -Xmn512m -XX:+UseParallelGC -XX:+UseAdaptiveSizePolicy";
	private String configurationFile = "./AUS/Input/Configuration/AUS_DIR_sw.cfg";
	private String jarfile = "Conn4D_bin.jar";
	private String logdir = "./logs/AUS";
	private String headerFile = "C:/Temp/header.sh";

	private String prefix = "SWT";
	private String saveDirPrefix = "Pilot_";
	private static String infile = "C:/Temp/incomplete.dat";
	private static String outfile = "C:/Temp/inc2.dat";
	private String restartFile = "C:/Temp/restartTest.sh";
	private String lastEntry = "SOW-43986";
	
	private String outputFolder = "G:/Temp/Output_sw/Pilot_D0-10";
	private int copyInterval=300;
	private String saveFolder="/g/data/dh8/modeling/AUS/Output_sw";
	
	public static void copyFile( File from, File to ) throws IOException {
	    Files.copy( from.toPath(), to.toPath() );
	}

	public void buildHaltFile(String root, String outputPath) throws IOException{
		FileWriter fw = new FileWriter(new File(outputPath));
		File dir = new File(root);
		File[] subdirs = dir.listFiles();
		Arrays.sort(subdirs);
		for(File subdir:subdirs){
			if(!subdir.isDirectory()){
				continue;
			}
			File[] files = subdir.listFiles(new EndsWithFilter(".dat"));
			Arrays.sort(files);
			for(File file:files){
				System.out.println("Working on " + file.getName());
				setInputFile(file);
				fw.write(file.getPath() + "\t" + getLastRelease() + "\n");
			}
		}
		fw.flush();
		fw.close();
	}
	
	private static void copyFileUsingStream(File source, File dest) throws IOException {
	    InputStream is = null;
	    OutputStream os = null;
	    try {
	        is = new FileInputStream(source);
	        os = new FileOutputStream(dest);
	        byte[] buffer = new byte[1024];
	        int length;
	        while ((length = is.read(buffer)) > 0) {
	            os.write(buffer, 0, length);
	        }
	    } finally {
	        is.close();
	        os.close();
	    }
	}
	
	public static void main(String[] args){
		//RestartTools rst = new RestartTools("C:/Temp/incomplete.dat");
		
		//RestartTools rst = new RestartTools(infile);
		RestartTools rst = new RestartTools();
		//rst.setInputFile(new File(infile));
		//rst.setOutputFile(new File(outfile));
		
		if(args.length < 2){
			System.out.println("Usage: <root directory> <output file>");
			System.exit(-1);
		}
		try {
			rst.buildHaltFile(args[0], args[1]);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//rst.walkDir("G:/Temp/Output_sw", 20, 37, 0, 32);
		System.out.println("Complete");
	}
	
	public RestartTools(){
		try {
			Date startDate = df.parse("2009-01-01 00:00:00 UTC");
			Date endDate = df.parse("2012-12-31 00:00:00 UTC");
			long interval = TimeConvert.daysToMillis(30);
			releaseDates=buildDates(startDate,endDate,interval);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	public RestartTools(File f){
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		setInputFile(f);
		setOutputFile(new File(outfile));
		try {
			Date startDate = df.parse("2009-01-01 00:00:00 UTC");
			Date endDate = df.parse("2012-12-31 00:00:00 UTC");
			long interval = TimeConvert.daysToMillis(30);
			releaseDates=buildDates(startDate,endDate,interval);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	public RestartTools(String s){
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		setInputFile(new File(s));
		setOutputFile(new File(outfile));
		try {
			Date startDate = df.parse("2009-01-01 00:00:00 UTC");
			Date endDate = df.parse("2012-12-31 00:00:00 UTC");
			long interval = TimeConvert.daysToMillis(30);
			releaseDates=buildDates(startDate,endDate,interval);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	public void close(){
		try {
			in.close();
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void createRestartFile(String header, int dateIndex, int depthIndex){
		File infile = new File(header);
		String outputName=outputFolder+"/"+sdf.format(releaseDates[dateIndex]) + "_" + mindepths[depthIndex]+"-"+maxdepths[depthIndex]+".prm";
		
		File outfile = new File(restartFile);
		try {
			copyFileUsingStream(infile,outfile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try (BufferedWriter bw= new BufferedWriter(new FileWriter(outfile,true));){
			bw.write("(java "+prms+" -jar " + jarfile + " "  + outputName + " " + configurationFile + " " + source + " > " + logdir + "/" + prefix + dateIndex + "D" + depthIndex + ".log)&\n");
			bw.write(cronString());
			
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	public DataItem getLastItem(){
		boolean EOF = false;
		DataItem lline = null;
		
		while (!EOF) {
		    try {
		        lline = read();
		    } catch (EOFException e) {
		        EOF = true;
		    }
		}
		return lline;
	}
	
	public String getLastRelease(){
		return getLastItem().getSource();
	}
	
	public String getNextFromShapefile(String lastitem){
		
		FeatureIterator<SimpleFeature> it = sr.getIterator();
		while(it.hasNext()){
			String nx = (String) it.next().getAttribute("FNAME");
			if(nx.equals(lastitem)){
				if(it.hasNext()){
					return (String) it.next().getAttribute("FNAME");
				}
			}
		}
		return "NONE";
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
	
	public DataItem read() throws EOFException{
		
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
	
	public void repair(){
		
		DataItem halted = getLastItem();
		source = halted.getSource();
		
		if(source.equalsIgnoreCase(lastEntry)){
			File inf = new File(infile);
			File outf = new File(outfile);
			try {
				Files.copy(inf.toPath(), outf.toPath());
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}
		
		reset();
		
		boolean stop = false;
		while (!stop) {
		    try {
		        DataItem item = read();
		        if(item.getSource().equals(source)){
		        	stop = true;
		        	continue;
		        }
		        
		    item.write(out);
		        
		    } catch (EOFException e) {
		        stop = true;
		    } catch (IOException e){
		    	System.out.println(e);
		    }
		}
	}
	
	public void replace(){
		File original = new File(infile);
		File newfile = new File(outfile);
		File temp = new File(infile + ".xxx");
		if(newfile.exists()){
			try {
				copyFileUsingStream(original,temp);
				original.delete();
				copyFileUsingStream(newfile,original);
				newfile.delete();
				temp.delete();
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	}
	
	public String cronString(){
		StringBuilder sb = new StringBuilder();
		sb.append("SLTIME=" + copyInterval + "\n");
		sb.append("HERE=" + saveFolder + "\n");
		sb.append("(while true\n  do\n    sleep $SLTIME\n    cp -r $PBS_JOBFS/. $HERE\n  done) &\n");
		sb.append("wait %1\n");
		sb.append("kill `jobs -p`\n");
		sb.append("cp -r $PBS_JOBFS/. $HERE\n");
		return sb.toString();
	}
	
	public void reset(){
		setInputFile(inf);
	}
	
	public void setInputFile(File input){
		inf = input;
		try {
			 in = new DataInputStream(new BufferedInputStream(
					new GZIPInputStream(new FileInputStream(input))));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void setOutputFile(File output){
		try {
			 out = new DataOutputStream(new BufferedOutputStream(
					new GZIPOutputStream(new FileOutputStream(output))));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Date[] buildDates(Date start, Date end, long interval){
		ArrayList<Date> dates = new ArrayList<Date>();
		long ct = start.getTime();
		while(ct<end.getTime()){
			dates.add(new Date(ct));
			ct+=interval;
		}
		return dates.toArray(new Date[]{});
	}
	
	public void setShapeFile(String shapefile){
		sr = new ShapefileReader(shapefile);
	}
	
	public void walkDir(String rootdir, int dateStartIdx, int dateEndIdx, int depthStartIdx, int depthEndIdx){
		for(int i = dateStartIdx; i < dateEndIdx; i++){
			for(int j = depthStartIdx; j < depthEndIdx; j++){
				String base = rootdir + "/" + saveDirPrefix + "D" + mindepths[j] + "-" + maxdepths[j] + "\\" + sdf.format(releaseDates[i]);
				File inf = new File(base+".dat");
				File outf = new File(base+".cln");
				if(!inf.exists()){
					System.out.println(inf.getPath() + " does not exist.  Skipping...");
					continue;
				}
				setInputFile(inf);
				setOutputFile(outf);
			    repair();
			    
			    //replace();
			    createRestartFile(headerFile, i,j);
			}
		}
	}
}

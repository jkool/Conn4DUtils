package au.gov.ga.conn4DUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class Density3D {

	private double north = 0;
	private double south = 0;
	private double west = 0;
	private double east = 0;
	private double top = 0;
	private double bottom = 0;
	private double xdim = 1000;
	private double ydim = 1000;
	private double zdim = 10;
	private int nrows = 1;
	private int ncols = 1;
	private int nlevels = 1;
	private int xfield = 6;
	private int yfield = 7;
	private int zfield = 8;
	
	public void readFile(String filename){
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File(filename)));
			String header = br.readLine();
			if (header==null){
				System.out.println(filename + " is empty.  Exiting");
				System.exit(-1);
			}
			
			String ln = br.readLine();
			while (ln!=null){
				String[] split = ln.split("\t");
				double x = Double.parseDouble(split[xfield]);
				double y = Double.parseDouble(split[yfield]);
				double z = Double.parseDouble(split[zfield]);
				int x_idx = (int) (x-west/xdim);
				int y_idx = (int) (y-south/ydim);
				int z_idx = (int) (z-bottom/zdim);
				br.readLine();
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e){
			
		}
	}
	
	
	public void go(){
		
		
		
	}
	
}

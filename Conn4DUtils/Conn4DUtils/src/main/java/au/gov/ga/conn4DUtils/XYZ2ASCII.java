package au.gov.ga.conn4DUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class XYZ2ASCII {

	private static String inputFile = "C:/Users/Vector Sigma/Downloads/vgpm.2003001.all.xyz";
	
	public static void main(String[] args){
		
		XYZ2ASCII xyz = new XYZ2ASCII();
		xyz.go(inputFile);
		System.out.println("Conversion to ASCII Complete.");
	}
	
	private void go(String inputFile){
		
		double minx = Double.POSITIVE_INFINITY,maxx = Double.NEGATIVE_INFINITY,miny = Double.POSITIVE_INFINITY,maxy = Double.NEGATIVE_INFINITY;
		int rowct = 0, colct = 0;
		double rowspace = 0, colspace = 0;
		boolean firstcorner = true;
		boolean firstrow = true;
		boolean firstdelta = true;
		double eps = 1E-8;
		
		try {
			File f = new File(inputFile);
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			
			String ln = br.readLine();
			ln = br.readLine();
			
			
			double prevX = Double.NaN;
			double prevY = Double.NaN;
			
			int ct = 0;
			
			while(ln!= null){
				StringTokenizer stk = new StringTokenizer(ln);
				double x = Double.parseDouble(stk.nextToken());
				
				if(x<minx){minx = x;}
				if(x>maxx){maxx = x;}
				
				double y = Double.parseDouble(stk.nextToken());
				
				if(y<miny){miny = y;}
				if(y>maxy){maxy = y;}
				
				if(firstcorner){
					prevX = x;
					prevY = y;
					firstcorner = false;
					ln = br.readLine();
					ct++;
					continue;
				}
				
				if(firstdelta){
					double dx = Math.abs(x-prevX);
					if(dx<eps){
						br.close();
						throw new IllegalArgumentException("Cell width cannot be 0");
					}
					colspace = dx;			
					prevX = x;
					prevY = y;
					firstdelta = false;
					ln = br.readLine();
					ct++;
					continue;
				}
				
				if(firstrow){
					
					double dx = Math.abs(x-prevX);
					if(dx-colspace>eps){
						br.close();
						throw new IllegalArgumentException("Column spacing is not consistent.");
					}
					
					double dy = Math.abs(y-prevY);
					if(dy < eps){
						
						ct++;
						prevX = x;
						prevY = y;
						ln = br.readLine();
						continue;
					}
					
					// else it's a new row, so save the number of columns we've traversed.
					
					colct = ct;
					ct = 0;
					prevX = x;
					prevY = y;
					firstrow = false;
					ln = br.readLine();
					continue;
				}
				
				double dx = Math.abs(x-prevX);
				if(dx-colspace>eps){
					br.close();
					throw new IllegalArgumentException("Column spacing is not consistent.");
				}
				
				
				
				ln = br.readLine();
			}
			
			System.out.println("#rows: " + rowct);
			System.out.println("#cols: " + colct);
			System.out.println("rowspace: " + rowspace);
			System.out.println("colspace: " + colspace);
			System.out.println("minx " + minx);
			System.out.println("maxx " + maxx);
			System.out.println("miny " + miny);
			System.out.println("maxy " + maxy);
			
			br.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
	}
	
}

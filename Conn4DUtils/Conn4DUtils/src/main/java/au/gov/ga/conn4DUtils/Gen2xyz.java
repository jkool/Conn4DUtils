package au.gov.ga.conn4DUtils;

	import java.io.*;
	import java.util.*;

	public class Gen2xyz {

	  public static void main(String[] args) {

	    convert("D:/temp/g25.gen");

	  }

	  private static void convert(String filename) {

	    StringBuffer sb = new StringBuffer(filename);
	    int dot = sb.indexOf(".");
	    String root = sb.substring(0, dot);
	    StringTokenizer stk;

	    try {

	      int altid = 1;
	      double x, y;
	      String identifier;
	      String ln;
	      BufferedReader bf = new BufferedReader(new FileReader(filename));
	      BufferedWriter bw = new BufferedWriter(new FileWriter(root + ".xyz"));

	      identifier = bf.readLine();
	      while (!identifier.equalsIgnoreCase("END")) {

	        ln = bf.readLine();

	        while (!ln.equalsIgnoreCase("END")) {

	          stk = new StringTokenizer(ln);
	          x = Double.parseDouble(stk.nextToken());
	          y = Double.parseDouble(stk.nextToken());

	          bw.write(altid + "\t" + x + "\t" + y + "\n");

	          ln = bf.readLine();

	        }

	        altid++;
	        identifier = bf.readLine();

	      }

	      bw.flush();
	      bw.close();
	      bf.close();

	    }
	    catch (NumberFormatException ex) {
	    }
	    catch (IOException ex) {
	      ex.printStackTrace();
	      System.out.print("The file name supplied could not be found or read.");
	    }
	  }
	}

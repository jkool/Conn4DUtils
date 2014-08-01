package au.gov.ga.conn4DUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;

/**
 * 
 * @author Johnathan Kool
 *
 */

public class MergeTRJTree {

	private static String dir = "X:/Modeling/SimOutput/IND/INDc_05_30";
	private static String year = "2008";
	private static String outpath = "D:/Modeling/SimOutput/IND/TRJ";
	private static String label = "GMC";
	private static int min = 240;
	private static int max = 299;
	private String extension = ".txt";
	private String separator = "-";
	private static int[] list = {};
	
	public static void main(String[] args){
		
		if(args!=null && args.length==6){
			dir = args[0];
			year = args[1];
			outpath = args[2];
			label = args[3];
			min = Integer.parseInt(args[4]);
			max = Integer.parseInt(args[5]);
		}
		
		MergeTRJTree mtt = new MergeTRJTree();
		mtt.exc();
		System.out.println("Complete.");
		
	}
	
	public void exc(){
		
		File fdir = new File(dir);
		FileWriter fw;
		BufferedWriter bw;
		
		try {
			
			fw = new FileWriter(outpath + "/" + label + year + ".txt");
			bw = new BufferedWriter(fw);
			
			FilenameFilter filter = new FilenameFilter() {
			    public boolean accept(File dir, String name) {
			        return !name.startsWith(".")&&name.startsWith(year);
			    }
			};
			
			File[] fa = fdir.listFiles(filter);
			FileReader fr;
			BufferedReader br;
			
			boolean first = true;
			
			for(File f:fa){
				
				String name = f.getName();
				TRJMerge tm = new TRJMerge();
				tm.setDirname(f.getAbsolutePath());
				tm.setOutput(outpath + "/" + name + ".txt");
				tm.setMin(min);
				tm.setMax(max);
				tm.go();
				
				int start = name.indexOf(separator);
				int end = name.lastIndexOf(extension);
				String substr = name.substring(start + 1, end);
				
				fr = new FileReader(outpath + "/" + name + ".txt");
				br = new BufferedReader(fr);
				
				String ln = br.readLine();
				if(first){
					bw.write("SOURCE\t" + ln + System.getProperty("line.separator"));
					first = false;
				}
				ln = br.readLine();
				while(ln != null){
					bw.write(substr + "\t" + ln + System.getProperty("line.separator"));
					ln = br.readLine();
				}
				System.out.println("Finished with " + name + ".");
				br.close();
				fr.close();
			}
			
			bw.flush();
			bw.close();
			fw.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String getDir() {
		return dir;
	}

	public static void setDir(String dir) {
		MergeTRJTree.dir = dir;
	}

	public static String getYear() {
		return year;
	}

	public static void setYear(String year) {
		MergeTRJTree.year = year;
	}

	public static String getOutpath() {
		return outpath;
	}

	public static void setOutpath(String outpath) {
		MergeTRJTree.outpath = outpath;
	}

	public static String getLabel() {
		return label;
	}

	public static void setLabel(String label) {
		MergeTRJTree.label = label;
	}

	public static int getMin() {
		return min;
	}

	public static void setMin(int min) {
		MergeTRJTree.min = min;
	}

	public static int getMax() {
		return max;
	}

	public static void setMax(int max) {
		MergeTRJTree.max = max;
	}
}

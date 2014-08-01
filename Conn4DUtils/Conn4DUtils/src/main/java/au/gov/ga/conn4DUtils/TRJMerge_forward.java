package au.gov.ga.conn4DUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;

public class TRJMerge_forward {
	public long minTime = Long.MIN_VALUE;
	public long maxTime = Long.MAX_VALUE;
	public String timeUnits;
	public Set<Long> polys = new TreeSet<Long>();
	public double minDepth = 0;
	public double maxDepth = Double.MAX_VALUE;
	public String rootPath = ".";
	public String outputPath = "output.txt";

	public List<File> select(String rootPath) {
		ArrayList<File> flist = new ArrayList<File>();
		File rootDir = new File(rootPath);
		File[] rootFiles = rootDir.listFiles(new DepthFilter(minDepth,maxDepth));
		for(File dir:rootFiles){
			if(!dir.isDirectory()){continue;}
			File[] files = dir.listFiles(new NumberFilter(polys));
			flist.addAll(Arrays.asList(files));
		}
		return flist;
	}

	public void process(List<File> flist, String outputPath) {
		boolean first = true;
		BufferedWriter bw = null;

		try {
			bw = new BufferedWriter(new FileWriter(outputPath));
		} catch (IOException e) {
			e.printStackTrace();
		}
		Iterator<File> it = flist.iterator();

		BufferedReader br = null;

		try {
			while (it.hasNext()) {
				br = new BufferedReader(new FileReader(it.next()));
				String ln = br.readLine();
				if (first) {
					first = false;
					bw.write(ln);
					ln = br.readLine();
				}
				while (ln != null) {
					bw.write(ln);
					ln = br.readLine();
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public long getMinTime() {
		return minTime;
	}

	public void setMinTime(long minTime) {
		this.minTime = minTime;
	}

	public long getMaxTime() {
		return maxTime;
	}

	public void setMaxTime(long maxTime) {
		this.maxTime = maxTime;
	}

	public String getTimeUnits() {
		return timeUnits;
	}

	public void setTimeUnits(String timeUnits) {
		this.timeUnits = timeUnits;
	}

	public Set<Long> getPolys() {
		return polys;
	}

	public void setPolys(Set<Long> polys) {
		this.polys = polys;
	}

	public double getMinDepth() {
		return minDepth;
	}

	public void setMinDepth(double minDepth) {
		this.minDepth = minDepth;
	}

	public double getMaxDepth() {
		return maxDepth;
	}

	public void setMaxDepth(double maxDepth) {
		this.maxDepth = maxDepth;
	}

	public String getRootPath() {
		return rootPath;
	}

	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	
	public static List<Long> range(int from, int to){
		List<Long> l = new ArrayList<Long>();
		for(long i = from; i < to+1; i++){
			l.add(i);
		}
		return l;
	}
}

class DepthFilter implements FilenameFilter{
	double min;
	double max;
	
	public DepthFilter(double min, double max){
		this.min = min;
		this.max = max;
	}
	
	@Override
	public boolean accept(File dir, String name) {
		int start = name.indexOf("_D");
		int end = name.length();
		String substr = name.substring(start,end);
		StringTokenizer stk = new StringTokenizer(substr,"-");
		double fmin = Double.parseDouble(stk.nextToken());
		double fmax = Double.parseDouble(stk.nextToken());
		
		if (((fmin>=min)&&(fmin<=max))||((fmax>=min)&&(fmax<=max))){
			return true;}
		return false;
	}
	
	public void setMin(double min){this.min = min;}
	public void setMax(double max){this.max = max;}
}

class NumberFilter implements FilenameFilter{
	public Set<Long> populations = new TreeSet<Long>();
	public NumberFilter(Set<Long> polys){
		populations = polys;
	}
	public void setPopulations(Set<Long> populations){
		this.populations = populations;
	}
	public Set<Long> getPopulations(){
		return populations;
	}
	public boolean accept(File dir, String name){
		int start = name.indexOf("A");
		int end = name.indexOf(".");
		String substr = name.substring(start,end);
		long pop = Long.parseLong(substr);
		if(populations.contains(pop)){return true;}
		return false;
	}
}
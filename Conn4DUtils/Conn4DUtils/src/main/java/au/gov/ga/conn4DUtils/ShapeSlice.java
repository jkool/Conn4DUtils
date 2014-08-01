package au.gov.ga.conn4DUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

public class ShapeSlice {

	private ArrayList<String> depthvals = new ArrayList<String>();
	private String minDepthFieldName = "MINDEPTH";
	private String maxDepthFieldName = "MAXDEPTH";
	private String inputFileName = "Z:/Modeling/AUS/Input/ReleaseFiles/gb_9k_g.shp";

	public ShapeSlice() {
		depthvals.add("0-10"); // 1
		depthvals.add("10-20"); // 2
		depthvals.add("20-30"); // 3
		depthvals.add("30-50"); // 4
		depthvals.add("50-75"); // 5
		depthvals.add("75-100"); // 6
		depthvals.add("100-125"); // 7
		depthvals.add("125-150"); // 8
		depthvals.add("150-200"); // 9
		depthvals.add("200-250"); // 10
		depthvals.add("250-300"); // 11
		depthvals.add("300-400"); // 12
		depthvals.add("400-500"); // 13
		depthvals.add("500-600"); // 14
		depthvals.add("600-700"); // 15
		depthvals.add("700-800"); // 16
		depthvals.add("800-900"); // 17
		depthvals.add("900-1000"); // 18
		depthvals.add("1000-1100"); // 19
		depthvals.add("1100-1200"); // 20
		depthvals.add("1200-1300"); // 21
		depthvals.add("1300-1400"); // 22
		depthvals.add("1400-1500"); // 23
		depthvals.add("1500-1750"); // 24
		depthvals.add("1750-2000"); // 25
		depthvals.add("2000-2500"); // 26
		depthvals.add("2500-3000"); // 27
		depthvals.add("3000-3500"); // 28
		depthvals.add("3500-4000"); // 29
		depthvals.add("4000-4500"); // 30
		depthvals.add("4500-5000"); // 31
		depthvals.add("5000-5500"); // 32
	}

	public void go(String filename) {

		File f = new File(filename);
		
		if(!f.isFile()){
			System.out.println(filename + " is not a file.  Exiting.");
			System.exit(0);
		}
		
		for (int i = 0; i < depthvals.size(); i++) {
			String range = depthvals.get(i);
			StringTokenizer stk = new StringTokenizer(range, "-\n");
			double mind = -Double.parseDouble(stk.nextToken());
			double maxd = -Double.parseDouble(stk.nextToken());

			ShapefileReader sr = new ShapefileReader(filename);
			String root = filename.substring(0, inputFileName.lastIndexOf("."));
			ShapefileWriter sw = new ShapefileWriter(sr.getSchema(), root + "_"
					+ range + ".shp");

			FeatureIterator<SimpleFeature> fi = sr.getIterator();
			List<SimpleFeature> features = new ArrayList<SimpleFeature>();

			while (fi.hasNext()) {
				SimpleFeature feature = fi.next();
				feature.setAttribute(minDepthFieldName, mind);
				feature.setAttribute(maxDepthFieldName, maxd);
				features.add(feature);
			}

			sw.write(features);
			System.out.println(depthvals.get(i) + " complete.");
		}
	}
	
	public static void main(String[] args){
		ShapeSlice ss = new ShapeSlice();
		ss.go(ss.inputFileName);
	}
}
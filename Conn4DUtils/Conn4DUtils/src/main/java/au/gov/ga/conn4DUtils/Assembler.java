package au.gov.ga.conn4DUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
//import java.net.URL;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

//import org.geotools.data.DataStore;
//import org.geotools.data.FeatureSource;
//import org.geotools.data.shapefile.ShapefileDataStore;
//import org.geotools.feature.FeatureCollection;
//import org.geotools.feature.FeatureIterator;
//import org.opengis.feature.simple.SimpleFeature;
//import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Assembles map string output into three-column sparse matrix format.
 * 
 * @author Johnathan Kool
 * 
 */

public class Assembler {

	private String lookupField = "FNAME";
	private String indexField = "POLYNUM";
	private String lookupTable;
	private String inputExtension = ".sum";
	private String inputDirectory;
	private String outputPath;
	private Map<String, Long> indexer = new TreeMap<String, Long>();
	private StringTokenizer stk;

	public static void main(String[] args) {
		Assembler asm = new Assembler();
		asm.setInputExtension(".sum");
		asm.setLookupTable("E:/HPC/Modeling/SimInput/IND/Shapefiles/IND_v2_g.shp");
		asm.setLookupField("FNAME");
		asm.setIndexField("POLYNUM");
		asm.setInputDirectory("F:/Temp/2009-01-04");
		asm.setOutputPath("c:/Temp/2009-01-04_d.mtx");
		asm.assemble();
	}

	public void assemble() {
		try {
			write(new File(outputPath), parse(new File(inputDirectory)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Processing of " + outputPath + " Complete.");
	}

	private Map<Long, Map<Long, Long>> parse(File directory) throws IOException {

		// URL shapeURL = new File(lookupTable).toURI().toURL();
		// DataStore store = new ShapefileDataStore(shapeURL);
		DBFReader dbr = new DBFReader(lookupTable);
		// String tname = store.getTypeNames()[0];
		// FeatureSource<SimpleFeatureType,SimpleFeature> source =
		// store.getFeatureSource(tname);
		// FeatureCollection<SimpleFeatureType,SimpleFeature> collection =
		// source.getFeatures();
		// FeatureIterator<SimpleFeature> iterator = collection.features();

		int lookup_column = dbr.columnLookup(lookupField);
		int index_column = dbr.columnLookup(indexField);

		Map<Long, Map<Long, Long>> sm = new TreeMap<Long, Map<Long, Long>>();

		// while (iterator.hasNext()) {
		while (dbr.hasNextRecord()) {
			// SimpleFeature ft = iterator.next();
			Object[] obj = dbr.nextRecord();
			indexer.put((String) obj[lookup_column],
					((Number) obj[index_column]).longValue());
			// indexer.put((String) ft.getAttribute(lookupField), ((Number) ft
			// .getAttribute(indexField)).longValue());
		}

		if (!directory.exists()) {
			System.out.println("ERROR:  Directory " + directory
					+ " does not exist.");
			System.exit(0);
		}

		Map<Long, Long> m;

		for (File f : directory.listFiles(new EndsWithFilter(inputExtension))) {

			// First look up the identifier
			String name = f.getName()
					.substring(0, f.getName().lastIndexOf("."));
			long where = -1;

			try {
				where = indexer.get(name);
			} catch (NullPointerException npe) {
				System.out.print("No lookup match was found for " + name
						+ " using field " + lookupField + " in " + lookupTable);
				System.out.println(" - Continuing...");
				continue;
			}

			if (sm.get(where) == null) {
				m = new TreeMap<Long, Long>();

			} else {
				m = sm.get(where);
			}

			BufferedReader br = new BufferedReader(new FileReader(f));
			stk = new StringTokenizer(br.readLine(), "[{, =}]");
			while (stk.hasMoreTokens()) {
				m.put(Long.parseLong(stk.nextToken()),
						Long.parseLong(stk.nextToken()));
			}
			sm.put(where, m);
			br.close();
		}
		return sm;
	}

	public Map<Long, Map<Long, Long>> read(File f) throws IOException {

		Map<Long, Map<Long, Long>> fm = new TreeMap<Long, Map<Long, Long>>();
		BufferedReader br = new BufferedReader(new FileReader(f));
		String ln = br.readLine();
		StringTokenizer stk;
		Long key1;
		while (ln != null) {
			stk = new StringTokenizer(ln);
			key1 = Long.parseLong(stk.nextToken());
			if (!fm.containsKey(key1)) {
				fm.put(key1, new TreeMap<Long, Long>());
			}
			fm.get(key1).put(Long.parseLong(stk.nextToken()),
					Long.parseLong(stk.nextToken()));
		}
		br.close();
		return fm;
	}

	public void write(File f, Map<Long, Map<Long, Long>> sm) throws IOException {

		BufferedWriter bw = new BufferedWriter(new FileWriter(f));
		Map<Long, Map<Long, Long>> tm = new TreeMap<Long, Map<Long, Long>>(sm);

		for (Long i : tm.keySet()) {
			for (Long j : tm.get(i).keySet()) {
				bw.write((i + 1) + "\t" + (j + 1) + "\t" + tm.get(i).get(j)
						+ "\n");
			}
		}
		bw.flush();
		bw.close();
	}

	public String getLookupField() {
		return lookupField;
	}

	public void setLookupField(String lookupField) {
		this.lookupField = lookupField;
	}

	public String getIndexField() {
		return indexField;
	}

	public void setIndexField(String indexField) {
		this.indexField = indexField;
	}

	public String getLookupTable() {
		return lookupTable;
	}

	public void setLookupTable(String table) {
		this.lookupTable = table;
	}

	public String getInputExtension() {
		return inputExtension;
	}

	public void setInputExtension(String extension) {
		this.inputExtension = extension;
	}

	public String getInputDirectory() {
		return inputDirectory;
	}

	public void setInputDirectory(String inputDirectory) {
		this.inputDirectory = inputDirectory;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
}

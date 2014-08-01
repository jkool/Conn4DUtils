package au.gov.ga.conn4DUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.geotools.data.DataStore;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Assembles map string output into three-column sparse matrix format.
 * 
 * @author Johnathan Kool
 * 
 */

public class AssemblerN {

	private String lookup = "FNAME";
	private String index = "POLYNUM";
	//private String table = "./default.shp";
	private String table = "X:/Modeling/SimInput/IND/Shapefiles/IND_v2_g.shp";
	private String extension = ".sum";
	//private String directory = "D:/Modeling/SimOutput/IND/IND_05_30_cor/2005-04-25";
	private String directory = "X:/Modeling/SimOutput/IND/IND_favg_05_30/2005-01-25";
	private String outputDir = "D:/Modeling/SimOutput/IND/Matrices/Favg";
	private String outputName = "2005-01-25.mtx";
	//private String directory = "./";
	//private String outputDir = "./";
	//private String outputName = "output";
	private String outext = ".mtx";
	private String output = outputDir + outputName + outext;
	private Map<String, Long> indexer = new TreeMap<String, Long>();
	private StringTokenizer stk;
	private static boolean dirOp = false;

	public static void main(String[] args) throws IOException {

		AssemblerN a = new AssemblerN();

		try {
			
			a.directory = args.length > 0 && !args[0].equalsIgnoreCase("#") ? args[0]
					: a.directory;
			a.table = args.length > 1 && !args[0].equalsIgnoreCase("#") ? args[1]
					: a.table;
			a.lookup = args.length > 2 && !args[0].equalsIgnoreCase("#") ? args[2]
					: a.lookup;
			a.index = args.length > 3 && !args[0].equalsIgnoreCase("#") ? args[3]
					: a.index;
			a.output = args.length > 4 && !args[0].equalsIgnoreCase("#") ? args[4]
					: a.output;
			a.extension = args.length > 5 && !args[0].equalsIgnoreCase("#") ? args[5]
					: a.extension;
			a.outext = args.length > 6 && !args[0].equalsIgnoreCase("#") ? args[6]
					: a.outext;
			if (args.length > 7 && args[7].equalsIgnoreCase("d")) {
				dirOp = true;
			}
			
		} catch (Exception e) {
			System.out
					.println("Input error.\n\nUsage: Assembler <directory> <lookup table> <lookup field> <ID field> <output location> <input extension> <output extension> <directory flag: d>");
			System.exit(0);
		}

		if (!dirOp) {
			Map<Long, Map<Long, Long>> map = a.parse(new File(a.directory));
			a.write(new File(a.output), map);
			System.out.println("Complete.");
		} else {
			File overDir = new File(a.directory);
			if (!new File(a.output).isDirectory()) {
				System.out
						.println("Output location "
								+ a.output
								+ " is not a directory.  Directory required for directory operations.");
			}
			File[] fileList = overDir.listFiles();
			for (File f : fileList) {
				if (f.isDirectory()) {
					try{
					Map<Long, Map<Long, Long>> map = a.parse(f);
					a.write(new File(a.output + File.separator + f.getName()
							+ a.outext), map);
					System.out.println("Completed " + f.getName());
					} catch (Exception e){
						System.out.println("ERROR parsing " + f.getName() + ".  Continuing.");
					}
				}
			}

		}
		System.exit(0);
	}
	
	public void execute() throws IOException{
		write(new File(outputName), parse(new File(directory)));
	}
	
	public void execute(File inputDirectory, String outputFile) throws IOException{
		write(new File(outputFile), parse(inputDirectory));
	}
	
	public void execute(String inputDirectory, String outputFile) throws IOException{
		write(new File(outputFile), parse(new File(inputDirectory)));
	}

	private Map<Long, Map<Long, Long>> parse(File directory)
			throws IOException {

		URL shapeURL = new File(table).toURI().toURL();
		DataStore store = new ShapefileDataStore(shapeURL);
		String tname = store.getTypeNames()[0];
		FeatureSource<SimpleFeatureType,SimpleFeature> source = store.getFeatureSource(tname);
		FeatureCollection<SimpleFeatureType,SimpleFeature> collection = source.getFeatures();
		Map<Long, Map<Long, Long>> sm = new TreeMap<Long, Map<Long, Long>>();
		FeatureIterator<SimpleFeature> iterator = collection.features();
		SimpleFeature ft;
		
		while (iterator.hasNext()) {
			ft = iterator.next();
			indexer.put((String) ft.getAttribute(lookup), ((Number) ft
					.getAttribute(index)).longValue());
		}
		
		if(!directory.exists()){
			System.out.println("ERROR:  Directory " + directory + " does not exist.");
			System.exit(0);
		}

		Map<Long, Long> m;

		for (File f : directory.listFiles(new EndsWithFilter(extension))) {

			// First look up the identifier
			String name = f.getName().substring(0, f.getName().lastIndexOf("."));
			long where = -1;

			try {
				where = indexer.get(name);
			} catch (NullPointerException npe) {
				System.out.print("No lookup match was found for " + name
						+ " using field " + lookup + " in " + table);
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
				m.put(Long.parseLong(stk.nextToken()), Long.parseLong(stk
						.nextToken()));
			}
			sm.put(where, m);
			br.close();
		}
		return sm;
	}

	public Map<Long, Map<Long, Long>> read(File f) throws IOException {

		TreeMap<Long, Map<Long, Long>> fm = new TreeMap<Long, Map<Long, Long>>();
		BufferedReader br = new BufferedReader(new FileReader(f));
		String ln = br.readLine();
		StringTokenizer stk;
		Long key1;
		while (ln != null) {
			stk = new StringTokenizer(ln);
			key1 = Long.parseLong(stk.nextToken());
			if(!fm.containsKey(key1)){
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
		TreeMap<Long, Map<Long, Long>> tm = new TreeMap<Long, Map<Long, Long>>(
				sm);

		for (Long i : tm.keySet()) {
			for (Long j : tm.get(i).keySet()) {
				bw.write((i + 1) + "\t" + (j + 1) + "\t" + tm.get(i).get(j)
						+ "\n");
			}
		}
		bw.flush();
		bw.close();
	}

	public String getLookup() {
		return lookup;
	}

	public void setLookup(String lookup) {
		this.lookup = lookup;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}

	public String getDirectory() {
		return directory;
	}

	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

	public String getOutputName() {
		return outputName;
	}

	public void setOutputName(String outputName) {
		this.outputName = outputName;
	}

	public String getOutext() {
		return outext;
	}

	public void setOutext(String outext) {
		this.outext = outext;
	}
}

package au.gov.ga.conn4DUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

public class Assemble_dir {

	String inputDir = "E:/HPC/Modeling/SimOutput/IND/INDc_05_30";
	String outputDir = "F:/Modeling/IND/Matrices/Connectivity/Coral";
	String lookupTable = "E:/HPC/Modeling/SimInput/IND/Shapefiles/IND_v2_g.shp";
	String rex = ".*";
	String ext = ".mtx";
	boolean overwrite = false;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Assemble_dir ad = new Assemble_dir();
		
		//if (args==null || args.length==0){
		//	System.out.println("Usage: java -jar BatchAssemble input directory> <output directory> <lookup table path> <regular expression filter> <overwrite=true/false>");
		//	System.out.println("Use # to use the default value");
		//	System.exit(1);
		//}
		
		ad.inputDir = args.length > 0 && !args[0].equalsIgnoreCase("#") ? args[0]
				: ad.inputDir;
		ad.outputDir = args.length > 1 && !args[1].equalsIgnoreCase("#") ? args[1]
				: ad.outputDir;
		ad.lookupTable = args.length > 2 && !args[2].equalsIgnoreCase("#") ? args[2]
				: ad.lookupTable;
		ad.rex = args.length > 3 && !args[3].equalsIgnoreCase("#") ? args[3]
				: ad.rex;	
		ad.overwrite = args.length > 4 ? Boolean.parseBoolean(args[4]) : ad.overwrite;
			
		ad.go();
		System.out.println("All files complete.");
	}

	public void go() {
		Assembler a = new Assembler();
		a.setInputExtension(".sum");
		a.setLookupTable(lookupTable);
		File fdir = new File(inputDir);
		File[] flist = fdir.listFiles(filter(rex));

			for (File f : flist) {
				String fname = outputDir + "/" + f.getName() + ext;
				if (new File(fname).exists() && !overwrite){
					System.out.println(fname + " exists.  Continuing...");
					continue;
				}
				a.setInputDirectory(f.getPath());
				a.setOutputPath(outputDir + "/" + f.getName() + ext);
				
				a.assemble();
			}
		}

	public static FilenameFilter filter(final String regex) {
		return new FilenameFilter() {
			private Pattern pattern = Pattern.compile(regex);

			public boolean accept(File dir, String name) {
				return pattern.matcher(new File(name).getName()).matches();
			}
		};
	}
}

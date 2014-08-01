package au.gov.ga.conn4DUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class TRJMerge_reverse2 {

	private int min = Integer.MIN_VALUE;// 2379;//Integer.MIN_VALUE;
	private int max = Integer.MAX_VALUE;// 2637;//Integer.MAX_VALUE;
	private static String dirname = "Z:/Temp/AUS_2005/2005-01-25";
	private String output = "Z:/Temp/test_rev2.txt";
	private String extension = ".trj";
	private boolean headerWrite = true;
	private static Set<Long> ids;
	private String pop;

	public static void main(String[] args) {
		TRJMerge_reverse2 tmr = new TRJMerge_reverse2();
		ids = new TreeSet<Long>();
		ids.add(6284l);
		ids.add(6285l);
		ids.addAll(range(6326, 6328));
		ids.addAll(range(6371, 6374));
		ids.addAll(range(6419, 6423));
		ids.addAll(range(6468, 6473));
		ids.addAll(range(6519, 6524));
		ids.addAll(range(6572, 6577));
		ids.addAll(range(6625, 6630));

		tmr.process(dirname);
		System.out.println("Complete.");
	}

	public void process(String dirname) {

		FileWriter fw = null;

		try {
			File dir = new File(dirname);
			fw = new FileWriter(output);
			BufferedWriter bw = new BufferedWriter(fw);

			File[] fa = dir.listFiles(new EndsWithFilter(extension));

			here: for (File f : fa) {

				String name = f.getName();
				System.out.print(name + "\t");

				BufferedReader br = new BufferedReader(new FileReader(f));
				br.readLine();
				String ln = br.readLine();

				while (ln != null) {

					StringTokenizer stk = new StringTokenizer(ln);
					stk.nextToken();
					stk.nextToken();
					stk.nextToken();
					stk.nextToken();
					stk.nextToken();
					stk.nextToken();
					stk.nextToken();
					stk.nextToken();
					pop = stk.nextToken();
					if(pop.equalsIgnoreCase("I") || pop.equalsIgnoreCase("L")){
						ln = br.readLine();
						continue;
					}
					pop = pop.substring(2, pop.length() - 1);

					if (!isNumeric(pop)) {
						ln = br.readLine();
						continue;
					}

					if (ids.contains(Long.parseLong(pop))) {
						System.out.print("writing...\t");
						String fileName = f.getPath();
						int end = fileName.lastIndexOf(".");
						String substring = fileName.substring(0, end);
						File trjfile = new File(substring + ".trj");
						BufferedReader br2 = new BufferedReader(new FileReader(
								trjfile));
						String ln2 = br2.readLine();
						if (headerWrite) {
							bw.write(ln2 + "\n");
							ln2 = br2.readLine();
							headerWrite = false;
						}
						while (ln2 != null) {
							bw.write(ln2 + "\n");
							ln2 = br2.readLine();
						}
						br2.close();
						continue here; // We're already writing the file so don't need
								// to check for other items in list
					}
					ln = br.readLine();
				}
				System.out.println(" done.");
				br.close();
			}

			bw.flush();
			bw.close();

		} catch (IOException e) {
			System.out.println("Error accessing output file: " + output);
			e.printStackTrace();
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					System.out.println("ERROR: " + output
							+ " could not be closed properly.");
					e.printStackTrace();
				}
			}
		}

		File ofile = new File(output);

		if (ofile.length() == 0) {
			System.out.println("\nWARNING: " + output + " length is 0\n");
		}

	}

	public String getDirname() {
		return dirname;
	}

	public void setDirname(String dirname) {
		this.dirname = dirname;
	}

	public String getOutput() {
		return output;
	}

	public void setOutput(String output) {
		this.output = output;
	}

	public int getMin() {
		return min;
	}

	public void setMin(int min) {
		this.min = min;
	}

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public boolean getHeaderWrite() {
		return headerWrite;
	}

	public void setHeaderWrite(boolean headerWrite) {
		this.headerWrite = headerWrite;
	}

	public static List<Long> range(int from, int to) {
		List<Long> l = new ArrayList<Long>();
		for (long i = from; i < to + 1; i++) {
			l.add(i);
		}
		return l;
	}

	public static boolean isNumeric(String str) {
		return str.matches("-?\\d+(\\.\\d+)?"); // match a number with optional
												// '-' and decimal.
	}
}

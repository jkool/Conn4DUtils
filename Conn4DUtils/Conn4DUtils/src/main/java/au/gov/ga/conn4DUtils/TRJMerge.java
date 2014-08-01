package au.gov.ga.conn4DUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class TRJMerge {

	private int min = Integer.MIN_VALUE;//2379;//Integer.MIN_VALUE;
	private int max = Integer.MAX_VALUE;//2637;//Integer.MAX_VALUE;
	private String dirname = "F:/Output";
	private String output = "Z:/Temp/test2.txt";
	private String extension = ".txt";
	private String separator = "-";
	private boolean headerWrite = true;

	public static void main(String[] args) {
		TRJMerge tm = new TRJMerge();
		tm.go();
		System.out.println("Complete.");
	}

	public void go() {

		FileWriter fw = null;

		try {
			File dir = new File(dirname);
			fw = new FileWriter(output);
			BufferedWriter bw = new BufferedWriter(fw);

			File[] fa = dir.listFiles(new EndsWithFilter(extension));

			for (File f : fa) {

				String name = f.getName();
				int start = name.indexOf(separator);
				int end = name.lastIndexOf(extension);
				String substr = name.substring(start + 1, end);
				int polynum = Integer.parseInt(substr);

				if (polynum >= min && polynum <= max) {

					FileReader fr = new FileReader(f);
					BufferedReader br = new BufferedReader(fr);

					String ln = br.readLine();
					if (headerWrite) {
						bw.write("SOURCE\t" + ln + System.getProperty("line.separator"));
						headerWrite = false;
					}
					ln = br.readLine();
					while (ln != null) {
						bw.write(substr + "\t" + ln + System.getProperty("line.separator"));
						ln = br.readLine();
					}
					br.close();
					fr.close();
				}
				System.out.println("File " + f.getName() + " processed.");
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
					System.out.println("ERROR: " + output + " could not be closed properly.");
					e.printStackTrace();
				}
			}
		}
		
		File ofile = new File(output);
		
		if(ofile.length()==0){
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
}

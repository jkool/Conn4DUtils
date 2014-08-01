package au.gov.ga.conn4DUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class TextFileIterator {

	String outputFile;// = "D:/SimOutput/NZ/2003-11-04.txt";
	String dir;// = "D:/SimOutput/NZ/NZ_30_30/2003-11-04";

	public static void main(String[] args) {
		TextFileIterator tfi = new TextFileIterator();
		tfi.go();
		System.out.println("Complete.");
	}

	public void go() {

		File fdir = new File(dir);
		File[] fa = fdir.listFiles(new EndsWithFilter(".txt"));
		boolean first = true;
		
		try {
			FileWriter fw = new FileWriter(outputFile);
			BufferedWriter bw = new BufferedWriter(fw);

			for (File f : fa) {
				try {
					FileReader fr = new FileReader(f);
					BufferedReader br = new BufferedReader(fr);
					
					String ln = br.readLine();
					
					if(first){
						bw.write("File" + "\t" + ln + "\n");
					}
					
					ln = br.readLine();
					
					while (ln != null) {
						bw.write(f.getName() + "\t" + ln + "\n");
						ln = br.readLine();
					}

					br.close();
					fr.close();

				} catch (FileNotFoundException e) {
					System.out.println("File: " + f
							+ " was not found.  Exiting.");
					System.exit(0);
				}
			}
			bw.flush();
			bw.close();
			
		} catch (IOException e) {
			System.out.println("Could not write to output file " + outputFile
					+ ".   Exiting.");
			System.exit(0);

		}
	}
	
	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public String getDir() {
		return dir;
	}

	public void setDir(String dir) {
		this.dir = dir;
	}
}

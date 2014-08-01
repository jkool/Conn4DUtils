package au.gov.ga.conn4DUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringTokenizer;

public class Struct2Arlq {

	String inputfile = "C:/Temp/long_run_reduced.txt";
	String outputfile = "C:/Temp/Testme4.txt";
	
	public static void main(String[] args){
		Struct2Arlq s2a = new Struct2Arlq();
		s2a.go();
		System.out.println("Complete.");
	}
	
	public void go(){
		
		boolean first = true;
		
		try {
			FileReader fr = new FileReader(inputfile);
			BufferedReader br = new BufferedReader(fr);
			FileWriter fw = new FileWriter(outputfile);
			BufferedWriter bw = new BufferedWriter(fw);
			
			int ct = 0;
			
			String ln = br.readLine();
			while(ln != null){
				StringTokenizer stk = new StringTokenizer(ln);
				int pop = Integer.parseInt(stk.nextToken());
				if(ct!=pop){
					if(!first){bw.write("}\n");}
					else{first = false;}
					bw.write("SampleName=\"Population " + ct + "\"\nSampleSize=50\nSampleData={\n");
					ct++;
				}
				bw.write(pop + "\t1\t");
				while(stk.hasMoreTokens()){
					bw.write(stk.nextToken()+"\t");
				}
				bw.write("\n");
				ln = br.readLine();
				stk = new StringTokenizer(ln);
				stk.nextToken();
				while(stk.hasMoreTokens()){
					bw.write(stk.nextToken()+"\t");
				}
				bw.write("\n");
				ln = br.readLine();
			}
		bw.write("}");
		bw.flush();
		bw.close();
		fw.close();
		br.close();
		fr.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
}

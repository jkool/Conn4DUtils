package au.gov.ga.conn4DUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class Convert_mtx {

	String inputFile = "D:/Marxan211/input_CERF/puvspr2.dat";
	String outputFile = "D:/Marxan211/input_CERF/puvspr2_rel.dat";
	Map<String,Map<String,String>> fm = new TreeMap<String,Map<String,String>>();
	boolean first=true;
	
	public static void main(String [] args){
		
		Convert_mtx cm = new Convert_mtx();
		cm.go();
		System.out.println("Complete.");
	}
	
	private void go(){
		
		try {
			FileReader fr = new FileReader(inputFile);
			BufferedReader br = new BufferedReader(fr);
			String ln = br.readLine();
			String[] species = {};
			
			while(ln != null){
				StringTokenizer stk = new StringTokenizer(ln,", \t");
				int nsp = stk.countTokens()-1;
				String tok;
				if(first){
					species = new String[nsp];
					stk.nextToken();
					for(int i = 0; i < nsp-1; i++){
						tok = stk.nextToken();
						species[i] = tok;
						fm.put(tok, new TreeMap<String,String>());
					}
					first = false;
				}

				else{
					String pu = stk.nextToken();
					for(int i = 0; i < nsp-1; i++){
						tok = stk.nextToken();
						if(Double.parseDouble(tok)>0){
							//fm.get(species[i]).put(pu, tok);
							fm.get(species[i]).put(pu, "1");
						}
					}
				}
				
				ln = br.readLine();
			}
			
			Iterator<String> it = fm.keySet().iterator();
			FileWriter fw = new FileWriter(outputFile);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write("species,pu,amount\n");
			
			while(it.hasNext()){
				
				String spec = it.next();
				Map<String,String> fm2 = fm.get(spec);
				Iterator<String> it2 = fm2.keySet().iterator();
				
				while(it2.hasNext()){
					String punit = it2.next();
					bw.write(spec + ", " + punit + ", " + fm2.get(punit)+"\n");
				}
				
			}
			
			br.close();
			bw.flush();
			bw.close();
			fw.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
}

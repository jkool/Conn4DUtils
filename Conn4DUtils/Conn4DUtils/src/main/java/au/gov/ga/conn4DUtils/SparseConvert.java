package au.gov.ga.conn4DUtils;

import java.util.*;
import java.io.*;

public class SparseConvert {
            
      public static Map<Number,Map<Number,Number>> convert(String filename) throws IOException{
            
            Map<Number,Map<Number,Number>> map = new TreeMap<Number,Map<Number,Number>>();
            FileReader fr = new FileReader(filename);
            BufferedReader br = new BufferedReader(fr);
            
            String ln = br.readLine();
            
            while(ln!=null){
                  StringTokenizer stk = new StringTokenizer(ln);
                  Number x = Integer.parseInt(stk.nextToken());
                  Number y = Integer.parseInt(stk.nextToken());
                  Number z = Double.parseDouble(stk.nextToken());
                  
                  if(map.containsKey(x)){
                        Map<Number,Number> fm = map.get(x);
                        fm.put(y, z);
                  }
                  else{
                        Map<Number,Number> fm = new TreeMap<Number,Number>();
                        fm.put(y, z);
                        map.put(x, fm);
                  }
                  
                  ln = br.readLine();
            }
            
            br.close();
            return map;
      }
}


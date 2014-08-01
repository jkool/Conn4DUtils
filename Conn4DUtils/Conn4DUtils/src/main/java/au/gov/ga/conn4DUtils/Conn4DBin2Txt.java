package au.gov.ga.conn4DUtils;

import java.io.EOFException;
import java.io.File;

public class Conn4DBin2Txt {
	
	Conn4DReader cr;
	Conn4DWriter cw;

	public static void main(String[] args) {
		Conn4DBin2Txt c4 = new Conn4DBin2Txt();
		c4.go();
		System.out.println("Complete");
	}

	public void go() {
		Conn4DBinaryReader cbr = new Conn4DBinaryReader();
		
		File dir = new File("V:/Projects/Modeling/AUS/Output/HAI");
		for(File f:dir.listFiles(new EndsWithFilter(".dat"))){
			String name = f.getPath().substring(0,f.getPath().lastIndexOf("."));
		
		cbr.open(f);
		cr = cbr;
		cw = new Conn4DTextWriter();
		cw.openTable(name + ".txt");
		cw.setReldepth(0);
		convertFile();
		System.out.println(name + " converted.");
		}
		//printFile();
	}
	
	public void convertFile(){
		try{
			while(true){
				cr.read();
				cw.setSource(cr.getSource());
				cw.setID(cr.getID());
				cw.setTime(cr.getTime());
				cw.setDuration(cr.getDuration());
				cw.setDepth(cr.getDepth());
				cw.setLon(cr.getLon());
				cw.setLat(cr.getLat());
				cw.setDistance(cr.getDistance());
				cw.setStatus(cr.getStatus());
				cw.setDestination(cr.getDestination());
				cw.setNodata(cr.isNodata());
				cw.write();
			}
		}
		catch (EOFException e){
		}	
		cr.close();
		cw.close();
	}
	
	public void printFile(){
		try{
			while(true){
				System.out.print(cr.readLine());
			}
		}
		catch (EOFException e){
		}	
		cr.close();
	}
}

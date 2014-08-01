package au.gov.ga.conn4DUtils;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TimeZone;

public class Binary2Text {

	File f;
	DataInputStream in;
	DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");

	public static void main(String[] args){
		Binary2Text b2 = new Binary2Text();
		b2.setFile(new File("G:/Model/Output/Latz/2010-12-01.dat"));
		b2.printFile();
		//b2.convert("G:/Model/Output/Latz/2010-12-01.dat", "G:/Model/Output/Latz/2010-12-01.txt");
		//System.out.println(b2.wasProperlyClosed());
		//System.out.println(b2.getLastLine());
		//b2.printFile();
	}
	
	public Binary2Text(){}
	
	public Binary2Text(File f){
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		setFile(f);
	}
	
	public Binary2Text(String s){
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		setFile(new File(s));
	}
	
	public void close(){
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String getLastLine(){
		try (RandomAccessFile file = new RandomAccessFile(f,"r")) {
			long index,length;
			length = file.length();
			index = length-1;
			int ch = 0;
			
			while(ch!=30 && index>0){
				file.seek(index--);
				ch = (file.read());
			}
			
			if(index==0){return null;}
			
			file.seek(index--);
			ch = (file.read());
			
			while(ch!=30 && index>0){
				file.seek(index--);
				ch = (file.read());
			}
			
			file.seek(index++);
			
			StringBuilder sb = new StringBuilder();
			
			sb.append(in.readUTF());
			sb.append("\t");
			in.readChar();
			sb.append(in.readLong());
			sb.append("\t");
			in.readChar();
			sb.append(df.format(new Date(in.readLong())));
			sb.append("\t");
			in.readChar();
			sb.append(TimeConvert.millisToDays(in.readLong()));
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			String code = in.readUTF();
			sb.append(code);
			if(code=="S"){
				sb.append(in.readUTF());
			}
			in.readChar();
			sb.append(in.readBoolean());

			return sb.toString();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
		return null;
	}
	
	public boolean wasProperlyClosed(){
		try (RandomAccessFile file = new RandomAccessFile(f,"r")) {
			long index,length;
			length = file.length();
			index = length-1;
			file.seek(index);
			return (file.read()==28);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
		return false;
	}
	
	public String getLastRelease(){
		StringTokenizer stk = new StringTokenizer(getLastLine());
		return stk.nextToken();
	}
	
	public void printFile(){
		boolean EOF = false;
		
		while (!EOF) {
		    try {
		        System.out.println(read());
		    } catch (EOFException e) {
		        EOF = true;
		    }
		} 
	}
	
	public void batchConvert(String directory){
		
		File dir = new File(directory);

		if (!dir.isDirectory()){
			System.out.println(directory + " is not a directory.  Exiting.");
			System.exit(-1);
		}
		
		for(File f:dir.listFiles()){
			String outName = f.getPath().substring(0, f.getPath().lastIndexOf("."))+".txt";
			convert(f,new File(outName));
		}
		
		System.out.println();
		System.out.println("Conversion of directory " + directory + " is complete.");
	}
	
	public void convert(String inputFile, String outputFile){
		convert(new File(inputFile), new File(outputFile));
	}
	
	public void convert(File inputFile, File outputFile){
		
		try {
			outputFile.createNewFile();
		} catch (IOException e1) {
			System.out.println("Could not create output file " + outputFile);
		}
		
		try (FileWriter bw = new FileWriter(outputFile)){
			setFile(inputFile);
			boolean EOF = false;
			
			bw.write("SOURCE\tID\tTIME_\tDURATION\tDEPTH\tLON\tLAT\tDISTANCE\tSTATUS\tNODATA\n");
			
			while (!EOF){
				try {
				    bw.write(read() + "\n");
				} catch (EOFException e){
					EOF = true;
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Input file " + inputFile + " converted to " + outputFile + ".");
	}
	
	public String read() throws EOFException{
		
		StringBuilder sb = new StringBuilder();
		
		try {
			sb.append(in.readUTF());
			sb.append("\t");
			in.readChar();
			sb.append(in.readLong());
			sb.append("\t");
			in.readChar();
			sb.append(df.format(new Date(in.readLong())));
			sb.append("\t");
			in.readChar();
			sb.append(TimeConvert.millisToDays(in.readLong()));
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			sb.append(in.readDouble());
			sb.append("\t");
			in.readChar();
			String code = in.readUTF();
			sb.append(code);
			if(code=="S"){
				sb.append(in.readUTF());
			}
			sb.append("\t");
			in.readChar();
			sb.append(in.readBoolean());
			in.readChar();
		} catch (EOFException eof) {
			throw new EOFException();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}
	
	public void setFile(File f){
		try {
			 this.f = f; 
			 in = new DataInputStream(new BufferedInputStream(
					//new GZIPInputStream(new FileInputStream(f))));
					new FileInputStream(f)));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		//} catch (IOException e) {
		//	e.printStackTrace();
		}
	}
}

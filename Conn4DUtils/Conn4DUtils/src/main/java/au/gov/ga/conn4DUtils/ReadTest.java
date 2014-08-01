package au.gov.ga.conn4DUtils;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ReadTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ReadTest rt = new ReadTest();
		try {
			rt.go();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void go() throws Exception{
		File f = new File("C:/Temp/incomplete.dat");
		ZipFile zf = new ZipFile("C:/Temp/incomplete.dat");
		Enumeration e = zf.entries();
		long sum = 0;
		while(e.hasMoreElements()){
		ZipEntry ze = (ZipEntry) e.nextElement();
		sum+=ze.getSize();
		}
		System.out.println(sum);
		DataInputStream is = new DataInputStream(new BufferedInputStream(
				new GZIPInputStream(new FileInputStream(f))));
		
	}

}

package au.gov.ga.conn4DUtils;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class TestAppend {

	private DataInputStream in;
	private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");
	private boolean negCoord = false;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TestAppend tp = new TestAppend();
		tp.setInputFile(new File("C:/Temp/incomplete.dat"));
		tp.append(new File("C:/Temp/incomplete2.dat"));
		tp.close();

	}

	public void append(File outputFile) {
		DataOutputStream fos = null;
		try {
			fos = new DataOutputStream((new GZIPOutputStream(new FileOutputStream(outputFile, true))));
			copy(in, fos);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (EOFException eof) {
		} catch (IOException e){
			e.printStackTrace();
		}finally {
			try {
				if (fos != null) {
					fos.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void copy(InputStream is, DataOutputStream os) throws EOFException {
		try {
		boolean EOF=false;
		while(!EOF){
	
			os.writeUTF(in.readUTF());
			os.writeChar(in.readChar());
			os.writeLong(in.readLong());
			os.writeChar(in.readChar());
			os.writeLong(in.readLong());
			os.writeChar(in.readChar());
			os.writeLong(in.readLong());
			os.writeChar(in.readChar());
			os.writeDouble(in.readDouble());
			os.writeChar(in.readChar());
			if (negCoord) {
				os.writeDouble(in.readDouble());
			} else {
				os.writeDouble(in.readDouble());
			}
			os.writeChar(in.readChar());
			os.writeDouble(in.readDouble());
			os.writeChar(in.readChar());
			os.writeDouble(in.readDouble());
			os.writeChar(in.readChar());
			os.writeUTF(in.readUTF());
			os.writeChar(in.readChar());
			os.writeBoolean(in.readBoolean());
			os.writeChar(in.readChar());
		}

		} catch (EOFException eof) {
			throw new EOFException();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setInputFile(File f) {
		try {
			in = new DataInputStream(new BufferedInputStream(
					new GZIPInputStream(new FileInputStream(f))));
			// new FileInputStream(f)));

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

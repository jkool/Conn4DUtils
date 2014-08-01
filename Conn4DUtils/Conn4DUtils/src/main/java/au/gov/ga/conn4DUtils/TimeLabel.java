package au.gov.ga.conn4DUtils;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import javax.imageio.ImageIO;

public class TimeLabel {

	private String directory = "C:/Temp/Animations/tmpnin";
	private String outputDir = "C:/Temp/Animations/tmpout";
	private String startDate = "2009/01/01";
	private DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
	private String fontName = "Arial";
	private int fontSize = 16;
	private long day = 1000 * 60 * 60 * 24;
	private int x_placement = 20;
	private int y_placement = 30;

	public static void main(String[] args) {

		TimeLabel tl = new TimeLabel();
		tl.burndir(tl.directory);
		System.out.println("Complete");
	}

	public void burndir(String dirName) {

		long basetime = 0;

		try {
			basetime = df.parse(startDate).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		File dir = new File(dirName);
		if (!dir.isDirectory()) {
			System.out.println(dirName + " is not a directory.  Exiting.");
			System.exit(1);
		}
		File[] files = dir.listFiles();
		Arrays.sort(files);

		for (int i = 0; i < files.length; i++) {
			burn(files[i].getPath(), df.format(new Date(basetime + i * day)),
					outputDir +"/"+ files[i].getName());
		}
	}

	public void burn(String inputFileName, String text, String outputFileName) {

		try {
			BufferedImage img = ImageIO.read(new File(inputFileName));
			int width = img.getWidth();
			int height = img.getHeight();

			BufferedImage bufferedImage = new BufferedImage(width, height,
					BufferedImage.TYPE_INT_RGB);
			Graphics2D g2d = bufferedImage.createGraphics();
			g2d.drawImage(img, 0, 0, null);

			g2d.setColor(Color.black);
			Font font = new Font(fontName, Font.PLAIN, fontSize);
			g2d.setFont(font);
			g2d.drawString(text, x_placement, y_placement);
			g2d.dispose();

			File outputfile = new File(outputFileName);
			ImageIO.write(bufferedImage, "jpg", outputfile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

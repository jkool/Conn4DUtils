package au.gov.ga.conn4DUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class Geom2Gen {

	public void geo2gen(List<Geometry> triangles, String output) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(output));

			for (int i = 0; i < triangles.size(); i++) {
				Geometry triangle = triangles.get(i);
				for (int j = 0; j < triangle.getCoordinates().length; j++) {
					bw.write(i + " ");
					Coordinate c = triangle.getCoordinates()[j];
					bw.write(c.x + " ");
					bw.write(c.y + " ");
					bw.write(Double.toString(c.z));
					bw.write("\n");
				}
			}
			bw.write("END\n");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (bw != null) {
				try {
					bw.flush();
					bw.close();
				} catch (IOException e) {
				}
			}
		}
	}	
}

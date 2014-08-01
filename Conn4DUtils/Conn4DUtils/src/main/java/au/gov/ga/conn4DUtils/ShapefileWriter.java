package au.gov.ga.conn4DUtils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class ShapefileWriter {

	private DataStore dataStore;
	private FeatureWriter<SimpleFeatureType, SimpleFeature> writer;
	private SimpleFeatureType type;
	
	public ShapefileWriter(String template, String path){
		ShapefileReader sr = new ShapefileReader(template);
		initialize(sr.getFeatureSource().getSchema(),path);
	}
	
	public ShapefileWriter(SimpleFeatureType type, String path) {
		initialize(type, path);
	}
	
	private void initialize(SimpleFeatureType type, String path){
		this.type = type;
		
		try {
			CoordinateReferenceSystem worldCRS = type
					.getCoordinateReferenceSystem();

			DataStoreFactorySpi factory = new ShapefileDataStoreFactory();
			Map<String, Serializable> create = new HashMap<String, Serializable>();
			create.put("url", new File(path).toURI().toURL());
			create.put("create spatial index", Boolean.TRUE);
			dataStore = factory.createNewDataStore(create);
			SimpleFeatureType featureType = SimpleFeatureTypeBuilder.retype(type,
					worldCRS);
			dataStore.createSchema(featureType);
			DefaultTransaction trans = new DefaultTransaction();
			writer =
		            dataStore.getFeatureWriterAppend(featureType.getTypeName(), trans);
			trans.close();
			//dataStore.dispose();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void write(List<SimpleFeature> features){
		try {
			SimpleFeatureCollection collection = new ListFeatureCollection(type,features);
			SimpleFeatureStore featureStore = (SimpleFeatureStore) dataStore.getFeatureSource(dataStore.getTypeNames()[0]);
			     featureStore.addFeatures(collection);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close(){
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

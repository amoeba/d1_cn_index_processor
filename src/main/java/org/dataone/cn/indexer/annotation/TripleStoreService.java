package org.dataone.cn.indexer.annotation;

import java.io.File;

import org.dataone.configuration.Settings;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.tdb.TDBFactory;

public class TripleStoreService {

	private static TripleStoreService instance;
	
	private TripleStoreService() {}
	
	public static TripleStoreService getInstance() {
		if (instance == null) {
			instance = new TripleStoreService();
		}
		return instance;
	}
	
	public Dataset getDataset() {
		String directory = Settings.getConfiguration().getString("index.tdb.directory", "./tdb");

    	// for testing, delete the triplestore each time
    	File dir = new File(directory);
    	if (!dir.exists()) {
        	dir.mkdirs();

    	}
//    	if (dir.exists()) {
//    		dir.delete();
//    	}
		Dataset dataset = TDBFactory.createDataset(directory);
		return dataset;
	}
}

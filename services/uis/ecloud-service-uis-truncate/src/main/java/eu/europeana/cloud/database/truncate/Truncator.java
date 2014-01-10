/* Truncator.java - created on Jan 10, 2014, Copyright (c) 2013 Europeana Foundation, all rights reserved */
package eu.europeana.cloud.database.truncate;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import eu.europeana.cloud.service.uis.database.DatabaseService;
import eu.europeana.cloud.service.uis.database.dao.util.DatabaseTruncateUtil;

/**
 * Database truncator class
 * 
 * @author Yorgos Mamakis (Yorgos.Mamakis@ europeana.eu)
 * @since Jan 10, 2014
 */
public class Truncator {

	private DatabaseService dbService;
	
	/**
	 * Creates a new instance of this class.
	 * @param dbService 
	 */
	public Truncator(DatabaseService dbService) {
		this.dbService = dbService;
	}

	
	/**
	 * Truncate the database
	 */
	public void truncate() {
		DatabaseTruncateUtil dbUtil = new DatabaseTruncateUtil(dbService);
		List<String> tables = new ArrayList<String>() {
			{
				add("data_providers");
				add("Cloud_Id");
				add("Provider_Record_Id");
			}
		};
		dbUtil.truncateTables(tables);
	}

}

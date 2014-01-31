/* Truncator.java - created on Jan 10, 2014, Copyright (c) 2013 Europeana Foundation, all rights reserved */
package eu.europeana.cloud.database.truncate;

import com.google.common.collect.ImmutableList;

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
	 * 
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
		dbUtil.truncateTables(ImmutableList.of("data_providers", "Cloud_Id", "Provider_Record_Id"));
	}

}

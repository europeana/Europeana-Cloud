/* DatabaseTruncateUtil.java - created on Jan 10, 2014, Copyright (c) 2013 Europeana Foundation, all rights reserved */
package eu.europeana.cloud.service.uis.persistent.util;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import java.util.List;


/**
 * Truncate table functionality for the Console application
 * 
 * @author Yorgos Mamakis (Yorgos.Mamakis@ europeana.eu)
 * @since Jan 10, 2014
 */
public final class DatabaseTruncateUtil {

	
	private CassandraConnectionProvider dbService;

	/**
	 * 
	 * Creates a new instance of this class.
	 * @param dbService 
	 */
	public DatabaseTruncateUtil(CassandraConnectionProvider dbService){
		this.dbService = dbService;
	}

	/**
	 * Truncate functionality
	 * @param tables
	 */
	public void truncateTables (List<String> tables){
		for(String table: tables){
			System.out.println("Truncating table " +table);
			dbService.getSession().execute("TRUNCATE " + table);
		}
	}
}

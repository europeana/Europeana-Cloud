package eu.europeana.cloud.service.uis.database;

import java.util.List;

import eu.europeana.cloud.exceptions.DatabaseConnectionException;
/**
 * Generic interface for Database Access. 
 * 
 * @author Yorgos.Mamakis@ kb.nl
 *
 * @param <T> CloudId or LocalId
 * @param <V> List<CloudId> or List<localId>
 */
public interface Dao <T , V extends List<T>>{

	/**
	 * Search on the database according to specific criteria
	 * @param deleted if record is deleted or not
	 * @param args The search criteria
	 * @return A List of objects T
	 * @throws DatabaseConnectionException
	 */
	V searchById(boolean deleted, String... args) throws DatabaseConnectionException;
	
	/**
	 * Convenience method that searches for records where deleted=false
	 * @param args The search criteria
	 * @return A List of objects T
	 * @throws DatabaseConnectionException
	 */
	V searchActive (String... args) throws DatabaseConnectionException;
	
	/**
	 * Insert a new record T
	 * @param args The values of T
	 * @return The updated list of objects T after inserting the new record
	 * @throws DatabaseConnectionException
	 */
	V insert (String... args) throws DatabaseConnectionException;
	
	/**
	 * Delete (soft) a record of type T
	 * @param args The criteria with which removal will be executed
	 * @throws DatabaseConnectionException
	 */
	void delete(String... args) throws DatabaseConnectionException;
	
	/**
	 * Update a record T based on specific criteria
	 * @param args The criteria to perform the update operation
	 * @throws DatabaseConnectionException
	 */
	void update(String... args) throws DatabaseConnectionException;
}

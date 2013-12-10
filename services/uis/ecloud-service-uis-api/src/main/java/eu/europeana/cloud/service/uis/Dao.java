package eu.europeana.cloud.service.uis;

import java.util.List;

import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.ProviderDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
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
	 * @throws ProviderDoesNotExistException 
	 * @throws CloudIdDoesNotExistException 
	 * @throws RecordDatasetEmptyException 
	 */
	V searchById(boolean deleted, String... args) throws DatabaseConnectionException, ProviderDoesNotExistException, CloudIdDoesNotExistException, RecordDatasetEmptyException;
	
	/**
	 * Convenience method that searches for records where deleted=false
	 * @param args The search criteria
	 * @return A List of objects T
	 * @throws DatabaseConnectionException
	 * @throws ProviderDoesNotExistException 
	 * @throws CloudIdDoesNotExistException 
	 * @throws RecordDatasetEmptyException 
	 */
	V searchActive (String... args) throws DatabaseConnectionException, ProviderDoesNotExistException, CloudIdDoesNotExistException, RecordDatasetEmptyException;
	
	/**
	 * Insert a new record T
	 * @param args The values of T
	 * @return The updated list of objects T after inserting the new record
	 * @throws DatabaseConnectionException
	 * @throws CloudIdDoesNotExistException 
	 * @throws RecordDatasetEmptyException 
	 * @throws ProviderDoesNotExistException 
	 */
	V insert (String... args) throws DatabaseConnectionException, CloudIdDoesNotExistException, ProviderDoesNotExistException, RecordDatasetEmptyException;
	
	/**
	 * Delete (soft) a record of type T
	 * @param args The criteria with which removal will be executed
	 * @throws DatabaseConnectionException
	 * @throws RecordIdDoesNotExistException 
	 * @throws ProviderDoesNotExistException 
	 */
	void delete(String... args) throws DatabaseConnectionException, ProviderDoesNotExistException, RecordIdDoesNotExistException;
	
	/**
	 * Update a record T based on specific criteria
	 * @param args The criteria to perform the update operation
	 * @throws DatabaseConnectionException
	 */
	void update(String... args) throws DatabaseConnectionException;
	
	/**
	 * Expose the host the of the server
	 * @return The host IP of the server
	 */
	String getHost();
	
	/**
	 * Expose the name of the keyspace the database 
	 * @return The keyspace of the database
	 */
	String getKeyspace();
	
	/**
	 * Expose the port of the server
	 * @return The port of the server
	 */
	String getPort();
}

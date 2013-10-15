package eu.europeana.cloud.uidservice.service;

import java.util.List;

import eu.europeana.cloud.definitions.model.GlobalId;
import eu.europeana.cloud.definitions.model.Provider;
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;

/**
 * Unique Identifier Service Interface This service specifies the available
 * methods for the successful generation and linking of records with a Global
 * eCloud Identifier
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public interface UniqueIdService {

	GlobalId create(String providerId, String recordId)
			throws DatabaseConnectionException, RecordExistsException;

	GlobalId search(String providerId, String recordId)
			throws DatabaseConnectionException, RecordDoesNotExistException;

	List<Provider> searchByGlobalId(String globalId)
			throws DatabaseConnectionException, GlobalIdDoesNotExistException;

	List<Provider> searchLocalIdsByProvider(String providerId, int start, int end)
			throws DatabaseConnectionException, ProviderDoesNotExistException,
			RecordDatasetEmptyException;

	List<GlobalId> searchGlobalIdsByProvider(String providerId, int start, int end)
			throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException;

	void createFromExisting(String globalId, String providerId, String recordId)
			throws DatabaseConnectionException, ProviderDoesNotExistException,
			GlobalIdDoesNotExistException, RecordIdDoesNotExistException,
			IdHasBeenMappedException;

	void removeMappingByLocalId(String providerId, String recordId)
			throws DatabaseConnectionException, ProviderDoesNotExistException,
			RecordIdDoesNotExistException;

	void deleteGlobalId(String globalId) throws DatabaseConnectionException,
			GlobalIdDoesNotExistException;
}

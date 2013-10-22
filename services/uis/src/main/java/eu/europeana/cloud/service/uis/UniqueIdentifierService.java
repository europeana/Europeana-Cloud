package eu.europeana.cloud.service.uis;

import java.util.List;

import eu.europeana.cloud.common.model.GlobalId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;

/**
 * Unique Identifier Service Interface This service specifies the available methods for the
 * successful generation and linking of records with a Global eCloud Identifier
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
public interface UniqueIdentifierService {
    /**
     * Create a Unique Identifier from the given providerId and recordId
     * 
     * @param providerId
     * @param recordId
     * @return The unique identifier for this record
     * @throws DatabaseConnectionException
     * @throws RecordExistsException
     */
    GlobalId createGlobalId(String providerId, String recordId) throws DatabaseConnectionException,
            RecordExistsException;

    /**
     * Search for a unique identifier based on the providerId and recordId
     * 
     * @param providerId
     * @param recordId
     * @return The unique identifier of the record
     * @throws DatabaseConnectionException
     * @throws RecordDoesNotExistException
     */
    GlobalId getGlobalId(String providerId, String recordId) throws DatabaseConnectionException,
            RecordDoesNotExistException;

    /**
     * Search all the records that are linked to a unique identifier
     * 
     * @param globalId
     * @return A list of providerIds with the records that have been linked to the unique identifier
     *         provided
     * @throws DatabaseConnectionException
     * @throws GlobalIdDoesNotExistException
     */
    List<LocalId> getLocalIdsByGlobalId(String globalId) throws DatabaseConnectionException,
            GlobalIdDoesNotExistException;

    /**
     * Retrieve the recordIds for a given provider, supporting pagination. If no pagination is
     * provided then the recordIds retrieved are 10000 starting from record 0
     * 
     * @param providerId
     * @param start
     * @param end
     * @return A list of recordIds for a provider bound to 10000 results
     * @throws DatabaseConnectionException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     */
    List<LocalId> getLocalIdsByProvider(String providerId, int start, int end)
            throws DatabaseConnectionException, ProviderDoesNotExistException,
            RecordDatasetEmptyException;

    /**
     * Retrieve the globalIds for a given provider, supporting pagination. If no pagination is
     * provided then the globalIds retrieved are 10000 starting from record 0
     * 
     * @param providerId
     * @param start
     * @param end
     * @return A list of globalIds for a provider bound to 10000 results
     * @throws DatabaseConnectionException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     */
    List<GlobalId> getGlobalIdsByProvider(String providerId, int start, int end)
            throws DatabaseConnectionException, ProviderDoesNotExistException,
            RecordDatasetEmptyException;

    /**
     * Create a mapping between a new providerId and recordId and an existing global identifier
     * 
     * @param globalId
     * @param providerId
     * @param recordId
     * @throws DatabaseConnectionException
     * @throws ProviderDoesNotExistException
     * @throws GlobalIdDoesNotExistException
     * @throws RecordIdDoesNotExistException
     * @throws IdHasBeenMappedException
     */
    void createIdMapping(String globalId, String providerId, String recordId)
            throws DatabaseConnectionException, ProviderDoesNotExistException,
            GlobalIdDoesNotExistException, RecordIdDoesNotExistException, IdHasBeenMappedException;

    /**
     * Remove the mapping between the providerId/recordId and the global identifier The mapping is
     * soft-deleted
     * 
     * @param providerId
     * @param recordId
     * @throws DatabaseConnectionException
     * @throws ProviderDoesNotExistException
     * @throws RecordIdDoesNotExistException
     */
    void removeIdMapping(String providerId, String recordId) throws DatabaseConnectionException,
            ProviderDoesNotExistException, RecordIdDoesNotExistException;

    /**
     * Delete a global Identifier and all of its relevant mappings. Everything is soft-deleted
     * 
     * @param globalId
     * @throws DatabaseConnectionException
     * @throws GlobalIdDoesNotExistException
     */
    void deleteGlobalId(String globalId) throws DatabaseConnectionException,
            GlobalIdDoesNotExistException;
}

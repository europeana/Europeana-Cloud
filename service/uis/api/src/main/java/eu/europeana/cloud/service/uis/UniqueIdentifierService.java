package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.service.uis.exception.*;

import java.util.List;

/**
 * Unique Identifier Service Interface This service specifies the available
 * methods for the successful generation and linking of records with a Cloud
 * eCloud Identifier
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
public interface UniqueIdentifierService {

    /**
     * Create a Unique Identifier from the given providerId and recordId
     * 
     * @param recordInfo
     *            providerId and optionally recordId
     * @return The unique identifier for this record
     * @throws DatabaseConnectionException
     * @throws RecordExistsException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     * @throws CloudIdDoesNotExistException
     */
    CloudId createCloudId(String... recordInfo)
            throws DatabaseConnectionException, RecordExistsException, ProviderDoesNotExistException,
            RecordDatasetEmptyException, CloudIdDoesNotExistException, CloudIdAlreadyExistException;


    /**
     * Search for a unique identifier based on the providerId and recordId
     * 
     * @param providerId
     * @param recordId
     * @return The unique identifier of the record
     * @throws DatabaseConnectionException
     * @throws RecordDoesNotExistException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     */
    CloudId getCloudId(String providerId, String recordId)
            throws DatabaseConnectionException, RecordDoesNotExistException, ProviderDoesNotExistException,
            RecordDatasetEmptyException;


    /**
     * Search all the records that are linked to a unique identifier
     * 
     * @param cloudId
     * @return A list of providerIds with the records that have been linked to
     *         the unique identifier provided
     * @throws DatabaseConnectionException
     * @throws CloudIdDoesNotExistException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     */
    List<CloudId> getLocalIdsByCloudId(String cloudId)
            throws DatabaseConnectionException, CloudIdDoesNotExistException, ProviderDoesNotExistException,
            RecordDatasetEmptyException;

    /**
     * Create a mapping between a new providerId and recordId and an existing
     * cloud identifier
     * 
     * @param cloudId
     * @param providerId
     * @param recordId
     * @return The newly created CloudId
     * @throws DatabaseConnectionException
     * @throws CloudIdDoesNotExistException
     * @throws IdHasBeenMappedException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     */
    CloudId createIdMapping(String cloudId, String providerId, String recordId)
            throws DatabaseConnectionException, CloudIdDoesNotExistException, IdHasBeenMappedException,
            ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdAlreadyExistException;


    /**
     * Create a mapping between a new providerId and recordId and an existing
     * cloud identifier
     * 
     * @param cloudId
     * @param providerId
     * @return The newly created CloudId
     * @throws DatabaseConnectionException
     * @throws CloudIdDoesNotExistException
     * @throws IdHasBeenMappedException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     */
    CloudId createIdMapping(String cloudId, String providerId)
            throws DatabaseConnectionException, CloudIdDoesNotExistException, IdHasBeenMappedException,
            ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdAlreadyExistException;


    /**
     * Remove the mapping between the providerId/recordId and the cloud
     * identifier The mapping is soft-deleted
     * 
     * @param providerId
     * @param recordId
     * @throws DatabaseConnectionException
     * @throws ProviderDoesNotExistException
     * @throws RecordIdDoesNotExistException
     */
    void removeIdMapping(String providerId, String recordId)
            throws DatabaseConnectionException, ProviderDoesNotExistException, RecordIdDoesNotExistException;


    /**
     * Delete a cloud Identifier and all of its relevant mappings. Everything is
     * soft-deleted
     * 
     * @param cloudId
     * @throws DatabaseConnectionException
     * @throws CloudIdDoesNotExistException
     * @throws RecordIdDoesNotExistException
     * @throws ProviderDoesNotExistException
     */
    List<CloudId> deleteCloudId(String cloudId)
            throws DatabaseConnectionException, CloudIdDoesNotExistException, ProviderDoesNotExistException,
            RecordIdDoesNotExistException;


    /**
     * Expose information about the database host entry;
     * 
     * @return The host IP
     */
    String getHostList();


    /**
     * Expose information about the keyspaceName
     * 
     * @return The keyspace name
     */
    String getKeyspace();


    /**
     * Expose the port of the database
     * 
     * @return The database port
     */
    String getPort();

}

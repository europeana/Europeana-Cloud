package eu.europeana.cloud.service.uis.rest;

import javax.ws.rs.core.Response;

import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.ProviderDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;

/**
 * UniqueId REST API implementation
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
public interface UniqueIdResource {
    /**
     * Invoke the unique identifier creation REST call HTTP call: GET
     * 
     * @param providerId Provider Identifier
     * @param recordId Record Identifier
     * @return JSON/XML response with the unique Identifier or Error Message
     * @throws RecordExistsException 
     * @throws DatabaseConnectionException 
     * @throws CloudIdDoesNotExistException 
     * @throws RecordDatasetEmptyException 
     * @throws ProviderDoesNotExistException 
     */
    Response createCloudId(String providerId, String recordId) throws DatabaseConnectionException, RecordExistsException, ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdDoesNotExistException;

    /**
     * Invoke the unique Identifier search according to providerId/recordId combo REST call HTTP
     * call: GET
     * 
     * @param providerId Provider Identifier
     * @param recordId Record Identifier
     * @return JSON/XML response with the unique Identifier or Error Message
     * @throws RecordDoesNotExistException 
     * @throws DatabaseConnectionException 
     * @throws RecordDatasetEmptyException 
     * @throws ProviderDoesNotExistException 
     */
    Response getCloudId(String providerId, String recordId) throws DatabaseConnectionException, RecordDoesNotExistException, ProviderDoesNotExistException, RecordDatasetEmptyException;

    /**
     * Invoke the retrieval of providerId/recordId REST call HTTP call: GET
     * 
     * @param cloudId Cloud Identifier
     * @return JSON/XML response with the list of local ids organized by provider or Error Message
     * @throws CloudIdDoesNotExistException 
     * @throws DatabaseConnectionException 
     * @throws RecordDatasetEmptyException 
     * @throws ProviderDoesNotExistException 
     */
    Response getLocalIds(String cloudId) throws DatabaseConnectionException, CloudIdDoesNotExistException, ProviderDoesNotExistException, RecordDatasetEmptyException;

    /**
     * Invoke the retrieval of recordId based on a providerId REST call HTTP call: GET
     * 
     * @param providerId Provider Identifier
     * @param start Record to start pagination from
     * @param to Number of record to retrieve
     * @return JSON/XML response with the list of local ids of that provider or Error Message
     * @throws ProviderDoesNotExistException 
     * @throws DatabaseConnectionException 
     * @throws RecordDatasetEmptyException 
     */
    Response getLocalIdsByProvider(String providerId, String start, int to) throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException;

    /**
     * Invoke the retrieval of all unique identifiers for a giver provider HTTP call: GET
     * 
     * @param providerId Provider Identifier
     * @param start Record to start pagination from
     * @param to Number of records to retrieve
     * @return JSON/XML response with the list of unique ids of that provider or Error Message
     * @throws RecordDatasetEmptyException 
     * @throws ProviderDoesNotExistException 
     * @throws DatabaseConnectionException 
     */
    Response getCloudIdsByProvider(String providerId, String start, int to) throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException;

    /**
     * Create the mapping between an existing unique identifier and a providerId/recordId combo HTTP
     * call: GET
     * 
     * @param cloudId Cloud identifier
     * @param providerId Provider Identifier 
     * @param recordId Record Identifier
     * @return JSON/XML acknowledgement or Error Message
     * @throws ProviderDoesNotExistException 
     * @throws IdHasBeenMappedException 
     * @throws CloudIdDoesNotExistException 
     * @throws DatabaseConnectionException 
     * @throws RecordDatasetEmptyException 
     */
    Response createIdMapping(String cloudId, String providerId, String recordId) throws DatabaseConnectionException, CloudIdDoesNotExistException, IdHasBeenMappedException, ProviderDoesNotExistException, RecordDatasetEmptyException;

    /**
     * Remove the mapping between a providerId/recordId and a unique identifier HTTP call: DELETE
     * 
     * @param providerId Provider Identifier
     * @param recordId Record Identifier
     * @return JSON/XML acknowledgement or Error Message
     * @throws RecordIdDoesNotExistException 
     * @throws ProviderDoesNotExistException 
     * @throws DatabaseConnectionException 
     */
    Response removeIdMapping(String providerId, String recordId) throws DatabaseConnectionException, ProviderDoesNotExistException, RecordIdDoesNotExistException;

    /**
     * Remove the unique identifier and all of its mappings HTTP call: DELETE
     * 
     * @param cloudId Cloud Identifier
     * @return JSON/XML acknowledgement or Error Message
     * @throws CloudIdDoesNotExistException 
     * @throws DatabaseConnectionException 
     * @throws RecordIdDoesNotExistException 
     * @throws ProviderDoesNotExistException 
     */
    Response deleteCloudId(String cloudId) throws DatabaseConnectionException, CloudIdDoesNotExistException, ProviderDoesNotExistException, RecordIdDoesNotExistException;
    
    /**
     * Invoke the unique identifier creation REST call HTTP call: GET
     * 
     * @param providerId Provider Identifier
     * @return JSON/XML response with the unique Identifier or Error Message
     * @throws RecordExistsException 
     * @throws DatabaseConnectionException 
     * @throws CloudIdDoesNotExistException 
     * @throws RecordDatasetEmptyException 
     * @throws ProviderDoesNotExistException 
     */
	Response createCloudId(String providerId) throws DatabaseConnectionException, RecordExistsException, ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdDoesNotExistException;
}

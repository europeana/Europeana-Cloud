package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.service.uis.exception.CloudIdAlreadyExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import java.util.List;

/**
 * Unique Identifier Service Interface This service specifies the available methods for the successful generation and linking of
 * records with a Cloud eCloud Identifier
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
public interface UniqueIdentifierService {

  /**
   * Create a Unique Identifier from the given providerId and recordId
   *
   * @param recordInfo providerId and optionally recordId
   * @return The unique identifier for this record
   * @throws DatabaseConnectionException Problems with connection to database
   * @throws RecordExistsException Given record exists
   * @throws ProviderDoesNotExistException Provider does not exist
   * @throws RecordDatasetEmptyException Dataset for record is empty
   * @throws CloudIdDoesNotExistException Cloud identifier does not exist
   */
  CloudId createCloudId(String providerId, String recordInfo)
      throws DatabaseConnectionException, RecordExistsException, ProviderDoesNotExistException,
      RecordDatasetEmptyException, CloudIdDoesNotExistException, CloudIdAlreadyExistException;

  /**
   * Create a Unique Identifier from the given providerId
   *
   * @return The unique identifier for this record
   * @throws DatabaseConnectionException Problems with connection to database
   * @throws RecordExistsException Given record exists
   * @throws ProviderDoesNotExistException Provider does not exist
   * @throws RecordDatasetEmptyException Dataset for record is empty
   * @throws CloudIdDoesNotExistException Cloud identifier does not exist
   */
  CloudId createCloudId(String providerId)
      throws DatabaseConnectionException, RecordExistsException, ProviderDoesNotExistException,
      RecordDatasetEmptyException, CloudIdDoesNotExistException, CloudIdAlreadyExistException;

  /**
   * Search for a unique identifier based on the providerId and recordId
   *
   * @param providerId Provider identifier
   * @param recordId Record identifier
   * @return The unique identifier of the record
   * @throws DatabaseConnectionException Problems with connection to database
   * @throws RecordDoesNotExistException Record does not exist
   * @throws ProviderDoesNotExistException Provider does not exist
   * @throws RecordDatasetEmptyException Dataset for record is empty
   */
  CloudId getCloudId(String providerId, String recordId)
      throws DatabaseConnectionException, RecordDoesNotExistException, ProviderDoesNotExistException,
      RecordDatasetEmptyException;


  /**
   * Search all the records that are linked to a unique identifier
   *
   * @param cloudId Cloud identifier
   * @return A list of providerIds with the records that have been linked to the unique identifier provided
   * @throws DatabaseConnectionException Problems with connection to database
   * @throws CloudIdDoesNotExistException Cloud identifier does not exist
   * @throws ProviderDoesNotExistException Provider does not exist
   * @throws RecordDatasetEmptyException Dataset for record is empty
   */
  List<CloudId> getLocalIdsByCloudId(String cloudId)
      throws DatabaseConnectionException, CloudIdDoesNotExistException, ProviderDoesNotExistException,
      RecordDatasetEmptyException;

  /**
   * Create a mapping between a new providerId and recordId and an existing cloud identifier
   *
   * @param cloudId Cloud identifier
   * @param providerId Provider identifier
   * @param recordId Record identifier
   * @return The newly created CloudId
   * @throws DatabaseConnectionException Problems with connection to database
   * @throws CloudIdDoesNotExistException Cloud identifier does not exist
   * @throws ProviderDoesNotExistException Provider does not exist
   * @throws RecordDatasetEmptyException Dataset for record is empty
   */
  CloudId createIdMapping(String cloudId, String providerId, String recordId)
      throws DatabaseConnectionException, CloudIdDoesNotExistException,
      ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdAlreadyExistException;


  /**
   * Create a mapping between a new providerId and recordId and an existing cloud identifier
   *
   * @param cloudId Cloud identifier
   * @param providerId Provider identifier
   * @return The newly created CloudId
   * @throws DatabaseConnectionException Problems with connection to database
   * @throws CloudIdDoesNotExistException Cloud identifier does not exist
   * @throws ProviderDoesNotExistException Provider does not exist
   * @throws RecordDatasetEmptyException Dataset for record is empty
   */
  CloudId createIdMapping(String cloudId, String providerId)
      throws DatabaseConnectionException, CloudIdDoesNotExistException,
      ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdAlreadyExistException;


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

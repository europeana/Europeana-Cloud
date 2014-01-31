package eu.europeana.cloud.client.uis.rest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.client.JerseyClientBuilder;

import eu.europeana.cloud.client.uis.rest.web.RelativeUrls;
import eu.europeana.cloud.client.uis.rest.web.UrlProvider;
import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

/**
 * The REST API client for the Unique Identifier Service
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class UISClient {

	private static Client client = JerseyClientBuilder.newClient();
	private static UrlProvider urlProvider;

	/**
	 * Creates a new instance of this class.
	 */
	public UISClient() {
		urlProvider = new UrlProvider();
	}

	/**
	 * Creates a new instance of this class.
	 * 
	 * @param uisUrl
	 */
	public UISClient(String uisUrl) {
		urlProvider = new UrlProvider(uisUrl);
	}

	/**
	 * Invoke the creation of a new CloudId REST call
	 * 
	 * @param providerId
	 *            The provider Id
	 * @param recordId
	 *            The record Id
	 * @return The newly generated CloudId
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	public CloudId createCloudId(String providerId, String recordId) throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.CREATECLOUDID.getUrl()))
				.queryParam(RelativeUrls.CREATECLOUDID.getParamNames().get(0), providerId)
				.queryParam(RelativeUrls.CREATECLOUDID.getParamNames().get(1), recordId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(CloudId.class);
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Invoke the creation of a new CloudId REST call
	 * 
	 * @param providerId
	 *            The provider Id
	 * @return The newly generated CloudId
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	public CloudId createCloudId(String providerId) throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.CREATECLOUDIDNOLOCAL.getUrl()))
				.queryParam(RelativeUrls.CREATECLOUDIDNOLOCAL.getParamNames().get(0), providerId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(CloudId.class);
		} else {
			try {
				throw generateException(resp.readEntity(ErrorInfo.class));
			} catch (Exception e) {
				throw e;
			}
		}
	}

	/**
	 * Invoke the retrieval of a cloud identifier
	 * 
	 * @param providerId
	 *            The provider Id
	 * @param recordId
	 *            The record Id
	 * @return The retrieved cloud Id
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	public CloudId getCloudId(String providerId, String recordId) throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.GETCLOUDID.getUrl()))
				.queryParam(RelativeUrls.GETCLOUDID.getParamNames().get(0), providerId)
				.queryParam(RelativeUrls.GETCLOUDID.getParamNames().get(1), recordId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(CloudId.class);
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Retrieve the local identifiers associated with a cloud identifier
	 * 
	 * @param globalId
	 *            The cloud id to search for
	 * @return The List of local ids associated with the cloud id
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	@SuppressWarnings("unchecked")
	public ResultSlice<CloudId> getRecordId(String globalId) throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.GETLOCALIDS.getUrl()))
				.queryParam(RelativeUrls.GETLOCALIDS.getParamNames().get(0), globalId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(ResultSlice.class);
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Retrieve records associated with a provider
	 * 
	 * @param providerId
	 *            The provider Id
	 * @return The List of Local ids associated with a provider
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	@SuppressWarnings("unchecked")
	public ResultSlice<LocalId> getRecordIdsByProvider(String providerId) throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.GETLOCALIDSBYPROVIDER.getUrl()))
				.queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(0), providerId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(ResultSlice.class);
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Retrieve the Cloud ids associated with a provider
	 * 
	 * @param providerId
	 *            The provider id
	 * @return The list of cloud ids associated with the provider id
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	@SuppressWarnings("unchecked")
	public ResultSlice<CloudId> getCloudIdsByProvider(String providerId) throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.GETCLOUDIDSBYPROVIDER.getUrl()))
				.queryParam(RelativeUrls.GETCLOUDIDSBYPROVIDER.getParamNames().get(0), providerId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(ResultSlice.class);
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Retrieve the local ids associated with a provider with pagination
	 * 
	 * @param providerId
	 *            The provider id
	 * @param recordId
	 *            The record id to start retrieval from
	 * @param window
	 *            The maximum number of records to fetch
	 * @return A list of local ids associated with the provider
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	@SuppressWarnings("unchecked")
	public ResultSlice<LocalId> getRecordIdsByProviderWithPagination(String providerId, String recordId, int window)
			throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.GETLOCALIDSBYPROVIDER.getUrl()))
				.queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(0), providerId)
				.queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(1), recordId)
				.queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(2), window).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(ResultSlice.class);
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Retrieve the cloud ids associated with a provider with pagination
	 * 
	 * @param providerId
	 *            The provider id
	 * @param cloudId
	 *            The cloud id to start retrieval from
	 * @param window
	 *            The maximum number of records to fetch
	 * @return A list of cloud ids associated with the provider
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	@SuppressWarnings("unchecked")
	public ResultSlice<CloudId> getCloudIdsByProviderWithPagination(String providerId, String cloudId, int window)
			throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.GETCLOUDIDSBYPROVIDER.getUrl()))
				.queryParam(RelativeUrls.GETCLOUDIDSBYPROVIDER.getParamNames().get(0), providerId)
				.queryParam(RelativeUrls.GETCLOUDIDSBYPROVIDER.getParamNames().get(1), cloudId)
				.queryParam(RelativeUrls.GETCLOUDIDSBYPROVIDER.getParamNames().get(2), window).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(ResultSlice.class);
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Create a mapping between a cloud id and provider and record id
	 * 
	 * @param globalId
	 *            The cloud id
	 * @param providerId
	 *            The provider id
	 * @param recordId
	 *            The record id
	 * @return A confirmation that the mapping has been created
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	public boolean createMapping(String globalId, String providerId, String recordId) throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.CREATEMAPPING.getUrl()))
				.queryParam(RelativeUrls.CREATEMAPPING.getParamNames().get(0), globalId)
				.queryParam(RelativeUrls.CREATEMAPPING.getParamNames().get(1), providerId)
				.queryParam(RelativeUrls.CREATEMAPPING.getParamNames().get(2), recordId).request().get();
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return true;
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}

	}

	/**
	 * Remove the association of a record id to a cloud id
	 * 
	 * @param providerId
	 *            The provider id to use
	 * @param recordId
	 *            The record id to use
	 * @return A confirmation that the mapping has removed correctly
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	public boolean removeMappingByLocalId(String providerId, String recordId) throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.REMOVEMAPPINGBYLOCALID.getUrl()))
				.queryParam(RelativeUrls.REMOVEMAPPINGBYLOCALID.getParamNames().get(0), providerId)
				.queryParam(RelativeUrls.REMOVEMAPPINGBYLOCALID.getParamNames().get(1), recordId).request().delete();
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return true;
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Delete a cloud id and all its mapped record ids
	 * 
	 * @param cloudId
	 *            The cloud id to remove
	 * @return A confirmation message that the mappings have been removed
	 *         correctly
	 * @throws CloudException
	 *             The generic cloud exception wrapper
	 */
	public boolean deleteCloudId(String cloudId) throws CloudException {
		Response resp = client.target(urlProvider.getUidUrl(RelativeUrls.DELETECLOUDID.getUrl()))
				.queryParam(RelativeUrls.REMOVEMAPPINGBYLOCALID.getParamNames().get(0), cloudId).request().delete();
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return true;
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Create a data provider
	 * 
	 * @param providerId
	 *            The data provider Id
	 * @param dp
	 *            The data provider properties
	 * @return A URL that points to the data provider
	 * @throws CloudException
	 */
	public String createProvider(String providerId, DataProviderProperties dp) throws CloudException {
		Response resp = client.target(urlProvider.getPidUrl(RelativeUrls.CREATEPROVIDER.getUrl()))
				.queryParam(RelativeUrls.CREATEPROVIDER.getParamNames().get(0), providerId).request()
				.post(Entity.json(dp));
		if (resp.getStatus() == Status.CREATED.getStatusCode()) {
			return resp.toString();
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Update a Data Provider
	 * 
	 * @param providerId
	 *            The provider to update
	 * @param dp
	 *            The data provider properties
	 * @return True if successful, false else
	 * @throws CloudException
	 */
	public boolean updateProvider(String providerId, DataProviderProperties dp) throws CloudException {
		Response resp = client
				.target(urlProvider.getPidUrl(urlProvider.getPidUrl("/" + RelativeUrls.CREATEPROVIDER.getUrl())))
				.path(providerId).request().post(Entity.json(dp));
		if (resp.getStatus() == Status.NO_CONTENT.getStatusCode()) {
			return true;
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Get data providers
	 * 
	 * @param startFrom
	 *            The record to start from
	 * @return A predefined number of data providers
	 * @throws CloudException
	 */
	@SuppressWarnings("unchecked")
	public ResultSlice<DataProvider> getDataProviders(String startFrom) throws CloudException {
		Response resp = client.target(urlProvider.getPidUrl(RelativeUrls.CREATEPROVIDER.getUrl()))
				.queryParam(RelativeUrls.RETRIEVEPROVIDERS.getParamNames().get(0), startFrom).request().get();
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(ResultSlice.class);
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Retrieve a selected data provider
	 * 
	 * @param providerId
	 *            The provider id to retrieve
	 * @return The Data provider that corresponds to the selected id
	 * @throws CloudException
	 */
	public DataProvider getDataProvider(String providerId) throws CloudException {
		Response resp = client.target(urlProvider.getPidUrl("/" + RelativeUrls.CREATEPROVIDER.getUrl()))
				.path(providerId).request().get();
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(DataProvider.class);
		} else {
			throw generateException(resp.readEntity(ErrorInfo.class));
		}
	}

	/**
	 * Generates the exception to be returned to the client
	 * 
	 * @param e
	 *            The error info that was generated
	 * @return A CloudException that wraps the original exception
	 */
	public CloudException generateException(ErrorInfo e) {
		IdentifierErrorTemplate error = IdentifierErrorTemplate.valueOf(e.getErrorCode());
		switch (error) {
		case CLOUDID_DOES_NOT_EXIST:
			return new CloudException(e.getErrorCode(), new CloudIdDoesNotExistException(e));
		case DATABASE_CONNECTION_ERROR:
			return new CloudException(e.getErrorCode(), new DatabaseConnectionException(e));
		case ID_HAS_BEEN_MAPPED:
			return new CloudException(e.getErrorCode(), new IdHasBeenMappedException(e));
		case PROVIDER_DOES_NOT_EXIST:
			return new CloudException(e.getErrorCode(), new ProviderDoesNotExistException(e));
		case PROVIDER_ALREADY_EXISTS:
			return new CloudException(e.getErrorCode(), new ProviderAlreadyExistsException(e));
		case RECORD_DOES_NOT_EXIST:
			return new CloudException(e.getErrorCode(), new RecordDoesNotExistException(e));
		case RECORD_EXISTS:
			return new CloudException(e.getErrorCode(), new RecordExistsException(e));
		case RECORDID_DOES_NOT_EXIST:
			return new CloudException(e.getErrorCode(), new RecordIdDoesNotExistException(e));
		case RECORDSET_EMPTY:
			return new CloudException(e.getErrorCode(), new RecordDatasetEmptyException(e));
		default:
			return new CloudException(e.getErrorCode(), new GenericException(e));
		}

	}
}

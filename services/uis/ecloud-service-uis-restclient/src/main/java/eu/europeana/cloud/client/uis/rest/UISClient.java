package eu.europeana.cloud.client.uis.rest;

import java.util.Date;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.client.JerseyClientBuilder;

import eu.europeana.cloud.client.uis.rest.web.RelativeUrls;
import eu.europeana.cloud.client.uis.rest.web.UrlProvider;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.uis.CloudIdList;
import eu.europeana.cloud.service.uis.LocalIdList;

/**
 * The REST API client for the Unique Identifier Service
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class UISClient {

	private static Client client = JerseyClientBuilder.newClient();
	private static UrlProvider urlProvider = new UrlProvider();
	
	/**
	 * Invoke the creation of a new CloudId REST call
	 * @param providerId The provider Id
	 * @param recordId The record Id
	 * @return The newly generated CloudId
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public CloudId createCloudId(String providerId, String recordId) throws CloudException {
		
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.CREATECLOUDID.getUrl()))
				.queryParam(RelativeUrls.CREATECLOUDID.getParamNames().get(0), providerId)
				.queryParam(RelativeUrls.CREATECLOUDID.getParamNames().get(1), recordId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(CloudId.class);
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}
	}

	/**
	 * Invoke the creation of a new CloudId REST call
	 * @param providerId The provider Id
	 * @return The newly generated CloudId
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public CloudId createCloudId(String providerId) throws CloudException {
		
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.CREATECLOUDIDNOLOCAL.getUrl()))
				.queryParam(RelativeUrls.CREATECLOUDIDNOLOCAL.getParamNames().get(0), providerId)
				.request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(CloudId.class);
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}
	}
	/**
	 * Invoke the retrieval of a cloud identifier
	 * @param providerId The provider Id
	 * @param recordId The record Id
	 * @return The retrieved cloud Id 
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public CloudId getCloudId(String providerId, String recordId) throws CloudException {
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.GETGLOBALID.getUrl()))
				.queryParam(RelativeUrls.GETGLOBALID.getParamNames().get(0), providerId)
				.queryParam(RelativeUrls.GETGLOBALID.getParamNames().get(1), recordId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return resp.readEntity(CloudId.class);
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}
	}

	/**
	 * Retrieve the local identifiers associated with a cloud identifier
	 * @param globalId The cloud id to search for
	 * @return The List of local ids associated with the cloud id
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public List<CloudId> getRecordId(String globalId) throws CloudException {
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.GETLOCALIDS.getUrl()))
				.queryParam(RelativeUrls.GETLOCALIDS.getParamNames().get(0), globalId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			CloudIdList cloudIds = resp.readEntity(CloudIdList.class);
			return cloudIds.getList();
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}
	}

	/**
	 * Retrieve records associated with a provider
	 * @param providerId The provider Id
	 * @return The List of Local ids associated with a provider
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public List<LocalId> getRecordIdsByProvider(String providerId) throws CloudException {
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.GETLOCALIDSBYPROVIDER.getUrl()))
				.queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(0), providerId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			LocalIdList localIds = resp.readEntity(LocalIdList.class);
			return localIds.getList();
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}
	}

	/**
	 * Retrieve the Cloud ids associated with a provider
	 * @param providerId The provider id
	 * @return The list of cloud ids associated with the procider id
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public List<CloudId> getCloudIdsByProvider(String providerId) throws CloudException {
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.GETGLOBALIDSBYPROVIDER.getUrl()))
				.queryParam(RelativeUrls.GETGLOBALIDSBYPROVIDER.getParamNames().get(0), providerId).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			CloudIdList cloudIds = resp.readEntity(CloudIdList.class);
			return cloudIds.getList();
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}
	}

	/**
	 * Retrieve the local ids associated with a provider with pagination
	 * @param providerId The provider id
	 * @param recordId The record id to start retrieval from
	 * @param window The maximum number of records to fetch
	 * @return A list of local ids associated with the provider
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public List<LocalId> getRecordIdsByProviderWithPagination(String providerId, String recordId, int window)
			throws CloudException {
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.GETLOCALIDSBYPROVIDER.getUrl()))
				.queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(0), providerId)
				.queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(1), recordId)
				.queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(2), window).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			LocalIdList localIds = resp.readEntity(LocalIdList.class);
			return localIds.getList();
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}
	}

	/**
	 * Retrieve the cloud ids associated with a provider with pagination
	 * @param providerId The provider id
	 * @param cloudId The cloud id to start retrieval from
	 * @param window The maximum number of records to fetch
	 * @return A list of cloud ids associated with the provider
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public List<CloudId> getCloudIdsByProviderWithPagination(String providerId, String cloudId, int window)
			throws CloudException {
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.GETGLOBALIDSBYPROVIDER.getUrl()))
				.queryParam(RelativeUrls.GETGLOBALIDSBYPROVIDER.getParamNames().get(0), providerId)
				.queryParam(RelativeUrls.GETGLOBALIDSBYPROVIDER.getParamNames().get(1), cloudId)
				.queryParam(RelativeUrls.GETGLOBALIDSBYPROVIDER.getParamNames().get(2), window).request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			CloudIdList cloudIds = resp.readEntity(CloudIdList.class);
			return cloudIds.getList();
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}
	}

	/**
	 * Create a mapping between a cloud id and provider and record id
	 * @param globalId The cloud id 
	 * @param providerId The provider id 
	 * @param recordId The record id
	 * @return A confirmation that the mapping has been created
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public boolean createMapping(String globalId, String providerId, String recordId) throws CloudException {
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.CREATEMAPPING.getUrl()))
				.queryParam(RelativeUrls.CREATEMAPPING.getParamNames().get(0), globalId)
				.queryParam(RelativeUrls.CREATEMAPPING.getParamNames().get(1), providerId)
				.queryParam(RelativeUrls.CREATEMAPPING.getParamNames().get(2), recordId).request().get();
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return true;
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}

	}

	/**
	 * Remove the association of a record id to a cloud id
	 * @param providerId The provider id to use
	 * @param recordId The record id to use
	 * @return A confirmation that the mapping has removed correctly
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public boolean removeMappingByLocalId(String providerId, String recordId) throws CloudException {
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.REMOVEMAPPINGBYLOCALID.getUrl()))
				.queryParam(RelativeUrls.REMOVEMAPPINGBYLOCALID.getParamNames().get(0), providerId)
				.queryParam(RelativeUrls.REMOVEMAPPINGBYLOCALID.getParamNames().get(1), recordId).request().delete();
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return true;
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}
	}
	
	/**
	 * Delete a cloud id and all its mapped record ids
	 * @param cloudId The cloud id to remove
	 * @return A confirmation message that the mappings have been removed correctly
	 * @throws CloudException The generic cloud exception wrapper
	 */
	public boolean deleteCloudId(String cloudId) throws CloudException {
		Response resp = client.target(urlProvider.createUrl(RelativeUrls.DELETEGLOBALID.getUrl()))
				.queryParam(RelativeUrls.REMOVEMAPPINGBYLOCALID.getParamNames().get(0), cloudId).request().delete();
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			return true;
		} else {
			ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode() + ":" + errorInfo.getDetails());
		}
	}
}

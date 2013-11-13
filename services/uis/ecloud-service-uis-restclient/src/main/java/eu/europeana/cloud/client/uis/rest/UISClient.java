package eu.europeana.cloud.client.uis.rest;

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

public class UISClient {

	private Client client = JerseyClientBuilder.newClient();

	public CloudId createCloudId(String providerId,String recordId) throws CloudException{
		Response resp = client.target(UrlProvider.createUrl(RelativeUrls.CREATERECORDID.getUrl())).
		queryParam(RelativeUrls.CREATERECORDID.getParamNames().get(0), providerId).
		queryParam(RelativeUrls.CREATERECORDID.getParamNames().get(1), recordId).request().get();
		
		if(resp.getStatus()== Status.OK.getStatusCode()){
			return resp.readEntity(CloudId.class);
		} else {
			ErrorInfo errorInfo =  resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode()+":"+errorInfo.getDetails());
		}
	}
	
	public CloudId getCloudId(String providerId,String recordId) throws CloudException{
		Response resp = client.target(UrlProvider.createUrl(RelativeUrls.GETGLOBALID.getUrl())).
		queryParam(RelativeUrls.GETGLOBALID.getParamNames().get(0), providerId).
		queryParam(RelativeUrls.GETGLOBALID.getParamNames().get(1), recordId).request().get();
		
		if(resp.getStatus()== Status.OK.getStatusCode()){
			return resp.readEntity(CloudId.class);
		} else {
			ErrorInfo errorInfo =  resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode()+":"+errorInfo.getDetails());
		}
	}
	
	public List<CloudId> getRecordId(String globalId) throws CloudException{
		Response resp = client.target(UrlProvider.createUrl(RelativeUrls.GETLOCALIDS.getUrl())).
		queryParam(RelativeUrls.GETLOCALIDS.getParamNames().get(0), globalId).request().get();
		
		if(resp.getStatus()== Status.OK.getStatusCode()){
			CloudIdList cloudIds = resp.readEntity(CloudIdList.class);
			return cloudIds.getList();
		} else {
			ErrorInfo errorInfo =  resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode()+":"+errorInfo.getDetails());
		}
	}
	
	public List<LocalId> getRecordIdsByProvider(String providerId) throws CloudException{
		Response resp = client.target(UrlProvider.createUrl(RelativeUrls.GETGLOBALIDSBYPROVIDER.getUrl())).
		queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(0), providerId).request().get();
		
		if(resp.getStatus()== Status.OK.getStatusCode()){
			LocalIdList localIds = resp.readEntity(LocalIdList.class);
			return localIds.getList();
		} else {
			ErrorInfo errorInfo =  resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode()+":"+errorInfo.getDetails());
		}
	}
	
	public List<CloudId> getCloudIdsByProvider(String providerId) throws CloudException{
		Response resp = client.target(UrlProvider.createUrl(RelativeUrls.GETGLOBALIDSBYPROVIDER.getUrl())).
		queryParam(RelativeUrls.GETGLOBALIDSBYPROVIDER.getParamNames().get(0), providerId).request().get();
		
		if(resp.getStatus()== Status.OK.getStatusCode()){
			CloudIdList cloudIds = resp.readEntity(CloudIdList.class);
			return cloudIds.getList();
		} else {
			ErrorInfo errorInfo =  resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode()+":"+errorInfo.getDetails());
		}
	}
	
	public List<LocalId> getRecordIdsByProviderWithPagination(String providerId,String recordId, int window) throws CloudException{
		Response resp = client.target(UrlProvider.createUrl(RelativeUrls.GETGLOBALIDSBYPROVIDER.getUrl())).
				queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(0), providerId).
				queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(1), recordId).
				queryParam(RelativeUrls.GETLOCALIDSBYPROVIDER.getParamNames().get(2), window).request().get();
		
		if(resp.getStatus()== Status.OK.getStatusCode()){
			LocalIdList localIds = resp.readEntity(LocalIdList.class);
			return localIds.getList();
		} else {
			ErrorInfo errorInfo =  resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode()+":"+errorInfo.getDetails());
		}
	}
	
	public List<CloudId> getCloudIdsByProvider(String providerId,String cloudId, int window) throws CloudException{
		Response resp = client.target(UrlProvider.createUrl(RelativeUrls.GETGLOBALIDSBYPROVIDER.getUrl())).
		queryParam(RelativeUrls.GETGLOBALIDSBYPROVIDER.getParamNames().get(0), providerId).
		queryParam(RelativeUrls.GETGLOBALIDSBYPROVIDER.getParamNames().get(1), cloudId).
		queryParam(RelativeUrls.GETGLOBALIDSBYPROVIDER.getParamNames().get(2), window).request().get();
		
		if(resp.getStatus()== Status.OK.getStatusCode()){
			CloudIdList cloudIds = resp.readEntity(CloudIdList.class);
			return cloudIds.getList();
		} else {
			ErrorInfo errorInfo =  resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode()+":"+errorInfo.getDetails());
		}
	}
	
	public String createMapping(String globalId,String providerId,String recordId) throws CloudException{
		Response resp = client.target(UrlProvider.createUrl(RelativeUrls.CREATEMAPPING.getUrl())).
				queryParam(RelativeUrls.CREATEMAPPING.getParamNames().get(0), globalId).
				queryParam(RelativeUrls.CREATEMAPPING.getParamNames().get(1), providerId).
				queryParam(RelativeUrls.CREATEMAPPING.getParamNames().get(2), recordId).request().get();
		if(resp.getStatus()== Status.OK.getStatusCode()){
			return "Mapping was created correctly";
		} else {
			ErrorInfo errorInfo =  resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode()+":"+errorInfo.getDetails());
		}
		
	}
	
	
	public String removeMappingByLocalId(String providerId,String recordId) throws CloudException{
		Response resp = client.target(UrlProvider.createUrl(RelativeUrls.REMOVEMAPPINGBYLOCALID.getUrl())).
				queryParam(RelativeUrls.REMOVEMAPPINGBYLOCALID.getParamNames().get(0), providerId).
				queryParam(RelativeUrls.REMOVEMAPPINGBYLOCALID.getParamNames().get(1), recordId).request().get();
		if(resp.getStatus()== Status.OK.getStatusCode()){
			return "Mapping removed correctly";
		} else {
			ErrorInfo errorInfo =  resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode()+":"+errorInfo.getDetails());
		}
	}
	
	public String deleteCloudId(String cloudId) throws CloudException{
		Response resp = client.target(UrlProvider.createUrl(RelativeUrls.DELETEGLOBALID.getUrl())).
				queryParam(RelativeUrls.REMOVEMAPPINGBYLOCALID.getParamNames().get(0), cloudId).request().get();
		if(resp.getStatus()== Status.OK.getStatusCode()){
			return "CloudId removed correctly";
		} else {
			ErrorInfo errorInfo =  resp.readEntity(ErrorInfo.class);
			throw new CloudException(errorInfo.getErrorCode()+":"+errorInfo.getDetails());
		}
	}
}

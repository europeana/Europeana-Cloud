package eu.europeana.cloud.service.uis.rest;

import com.qmino.miredot.annotations.ReturnType;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.CloudIdAlreadyExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource for DataProvider.
 * 
 */
@Path("/data-providers/{" + P_PROVIDER + "}")
@Component
@Scope("request")
public class DataProviderResource {

	@Autowired
	private UniqueIdentifierService uniqueIdentifierService;

	@Autowired
	private DataProviderService providerService;

	@Autowired
	private MutableAclService mutableAclService;

	private final String LOCAL_ID_CLASS_NAME = "LocalId";

	/**
	 * Gets provider.
	 * 
	 * @return Data provider.
	 * @throws ProviderDoesNotExistException
	 */
	@GET
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public DataProvider getProvider(@PathParam(P_PROVIDER) String providerId)
			throws ProviderDoesNotExistException {
		return providerService.getProvider(providerId);
	}

	/**
	 * Updates data provider information.
	 * 
	 * @param dataProviderProperties
	 *            data provider properties.
	 * @throws ProviderDoesNotExistException
	 * @statuscode 204 object has been updated.
	 */
	@PUT
	@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@PreAuthorize("hasPermission(#providerId, 'eu.europeana.cloud.common.model.DataProvider', write)")
	public void updateProvider(DataProviderProperties dataProviderProperties,
			@PathParam(P_PROVIDER) String providerId, @Context UriInfo uriInfo)
			throws ProviderDoesNotExistException {
		DataProvider provider = providerService.updateProvider(providerId,
				dataProviderProperties);
		EnrichUriUtil.enrich(uriInfo, provider);
	}

	/**
	 * Deletes data provider from database
	 * 
	 * @param dataProviderId
	 *            data provider id
	 * @return
	 * @throws ProviderDoesNotExistException
	 */
	@DELETE
	@PreAuthorize("hasRole('ROLE_ADMIN')")
	public Response deleteProvider(@PathParam(P_PROVIDER) String dataProviderId)
			throws ProviderDoesNotExistException {
		providerService.deleteProvider(dataProviderId);
		deleteProviderAcl(dataProviderId);
		return Response.ok().build();
	}

	/**
	 * Get the record identifiers for a specific provider identifier with
	 * pagination
	 * 
	 * @param from
	 * @param to
	 * @return A list of record Identifiers (with their cloud identifiers)
	 * @throws DatabaseConnectionException
	 * @throws ProviderDoesNotExistException
	 * @throws RecordDatasetEmptyException
	 */
	@GET
	@Path("localIds")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@ReturnType("eu.europeana.cloud.common.response.ResultSlice")
	public Response getLocalIdsByProvider(
			@PathParam(P_PROVIDER) String providerId,
			@QueryParam(UISParamConstants.Q_FROM) String from,
			@QueryParam(UISParamConstants.Q_TO) @DefaultValue("10000") int to)
			throws DatabaseConnectionException, ProviderDoesNotExistException,
			RecordDatasetEmptyException {

		ResultSlice<CloudId> pList = new ResultSlice<>();
		pList.setResults(uniqueIdentifierService.getLocalIdsByProvider(
				providerId, from, to));
		if (pList.getResults().size() == to) {
			pList.setNextSlice(pList.getResults().get(to - 1).getId());
		}
		return Response.ok(pList).build();
	}

	/**
	 * Get the cloud identifiers for a specific provider identifier with
	 * pagination
	 * 
	 * @param from
	 * @param to
	 * @return A list of cloud Identifiers
	 * @throws DatabaseConnectionException
	 * @throws ProviderDoesNotExistException
	 * @throws RecordDatasetEmptyException
	 */
	@GET
	@Path("cloudIds")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@ReturnType("eu.europeana.cloud.common.response.ResultSlice")
	public Response getCloudIdsByProvider(
			@PathParam(P_PROVIDER) String providerId,
			@QueryParam(UISParamConstants.Q_FROM) String from,
			@QueryParam(UISParamConstants.Q_TO) @DefaultValue("10000") int to)
			throws DatabaseConnectionException, ProviderDoesNotExistException,
			RecordDatasetEmptyException {
		ResultSlice<CloudId> pList = new ResultSlice<>();
		pList.setResults(uniqueIdentifierService.getCloudIdsByProvider(
				providerId, from, to));
		if (pList.getResults().size() == to) {
			pList.setNextSlice(pList.getResults().get(to - 1).getId());
		}
		return Response.ok(pList).build();
	}

	/**
	 * Create a mapping between a cloud identifier and a record identifier for a
	 * provider
	 * 
	 * @param localId
	 * @return The newly associated cloud identifier
	 * @throws DatabaseConnectionException
	 * @throws CloudIdDoesNotExistException
	 * @throws IdHasBeenMappedException
	 * @throws ProviderDoesNotExistException
	 * @throws RecordDatasetEmptyException
	 */
	@POST
	@Path("cloudIds/{" + P_CLOUDID + "}")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@PreAuthorize("isAuthenticated()")
	public Response createIdMapping(@PathParam(P_PROVIDER) String providerId,
			@PathParam(P_CLOUDID) String cloudId,
				@QueryParam(UISParamConstants.Q_RECORD_ID) String localId)
					throws DatabaseConnectionException, CloudIdDoesNotExistException,
						IdHasBeenMappedException, ProviderDoesNotExistException,
							RecordDatasetEmptyException, CloudIdAlreadyExistException {
		
		
		CloudId result = null;
		if (localId != null) {
			result = uniqueIdentifierService.createIdMapping(cloudId,
					providerId, localId);
		} else {
			result = uniqueIdentifierService.createIdMapping(cloudId,
					providerId);
		}

		String creatorName = SpringUserUtils.getUsername();
		String key = result.getLocalId().toString() + "/" + providerId;
		if (creatorName != null) {
		    ObjectIdentity providerIdentity = new ObjectIdentityImpl(LOCAL_ID_CLASS_NAME, key);
		    MutableAcl providerAcl = mutableAclService.createAcl(providerIdentity);

		    providerAcl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
		    providerAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
		    providerAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
		    providerAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName), true);

		    mutableAclService.updateAcl(providerAcl);
		}
		
		return Response
				.ok()
				.entity(result).build();
	}

	/**
	 * Remove the mapping between a record identifier and a cloud identifier
	 * 
	 * @return Confirmation that the mapping has been removed
	 * @throws DatabaseConnectionException
	 * @throws ProviderDoesNotExistException
	 * @throws RecordIdDoesNotExistException
	 */
	@DELETE
	@Path("localIds/{" + P_LOCALID + "}")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @PreAuthorize("hasPermission(#localId.concat('/').concat(#providerId),'"
    		+ LOCAL_ID_CLASS_NAME + "', write)")
	public Response removeIdMapping(@PathParam(P_PROVIDER) String providerId,
			@PathParam(P_LOCALID) String localId)
			throws DatabaseConnectionException, ProviderDoesNotExistException,
			RecordIdDoesNotExistException {
		uniqueIdentifierService.removeIdMapping(providerId, localId);
		return Response.ok("Mapping marked as deleted").build();
	}

	private void deleteProviderAcl(String dataProviderId) {

		ObjectIdentity providerIdentity = new ObjectIdentityImpl(
				DataProvider.class.getName(), dataProviderId);
		mutableAclService.deleteAcl(providerIdentity, false);
	}
}

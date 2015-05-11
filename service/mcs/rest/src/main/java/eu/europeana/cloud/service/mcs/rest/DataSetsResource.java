package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.common.web.ParamConstants.F_DATASET;
import static eu.europeana.cloud.common.web.ParamConstants.F_DESCRIPTION;
import static eu.europeana.cloud.common.web.ParamConstants.F_START_FROM;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.qmino.miredot.annotations.ReturnType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

/**
 * Resource to get and create data set.
 */
@Path("/data-providers/{" + P_PROVIDER + "}/data-sets")
@Component
@Scope("request")
public class DataSetsResource {

    @Autowired
    private DataSetService dataSetService;

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;

    @Autowired
    private MutableAclService mutableAclService;

    private final String DATASET_CLASS_NAME = DataSet.class.getName();

    /**
     * Returns all data sets for a provider. Result is returned in slices.
     * @summary get provider's data sets
     *
     * @param providerId  provider id for which returned data sets will belong to (required)
     * @param startFrom reference to next slice of result. If not provided,
     * first slice of result will be returned.
     * @return slice of data sets for given provider.
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<eu.europeana.cloud.common.model.DataSet>")
    public ResultSlice<DataSet> getDataSets(@PathParam(P_PROVIDER) String providerId,
    		@QueryParam(F_START_FROM) String startFrom) {
        return dataSetService.getDataSets(providerId, startFrom, numberOfElementsOnPage);
    }

    /**
     * Creates a new data set.
     *
     * @param providerId the provider for the created data set
     * @param dataSetId identifier of the data set (required).
     * @param description description of the data set.
     * @return URI to newly created data set in content-location.
     * @throws ProviderNotExistsException data provider does not exist.
     * @throws
     * eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException
     * data set with this id already exists
     * @statuscode 201 object has been created.
     */
    @POST
    @PreAuthorize("isAuthenticated()")
    public Response createDataSet(@Context UriInfo uriInfo,
    		@PathParam(P_PROVIDER) String providerId,
    		@FormParam(F_DATASET) String dataSetId, @FormParam(F_DESCRIPTION) String description)
            throws ProviderNotExistsException, DataSetAlreadyExistsException {
        ParamUtil.require(F_DATASET, dataSetId);

        DataSet dataSet = dataSetService.createDataSet(providerId, dataSetId, description);
        EnrichUriUtil.enrich(uriInfo, dataSet);
        final Response response = Response.created(dataSet.getUri()).build();
        
        String creatorName = SpringUserUtils.getUsername();
        if (creatorName != null) {
            ObjectIdentity dataSetIdentity = new ObjectIdentityImpl(DATASET_CLASS_NAME,
            		dataSetId + "/" + providerId);

            MutableAcl datasetAcl = mutableAclService.createAcl(dataSetIdentity);

            datasetAcl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
            datasetAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
            datasetAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
            datasetAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName),
                    true);

            mutableAclService.updateAcl(datasetAcl);
        }
        
        return response;
    }
}

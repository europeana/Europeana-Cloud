package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.common.web.ParamConstants.F_DESCRIPTION;
import static eu.europeana.cloud.common.web.ParamConstants.F_START_FROM;
import static eu.europeana.cloud.common.web.ParamConstants.P_DATASET;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.qmino.miredot.annotations.ReturnType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;

/**
 * Resource to manage data sets.
 */
@Path("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}")
@Component
@Scope("request")
public class DataSetResource {

    @Autowired
    private DataSetService dataSetService;
    
    @Autowired
    private MutableAclService mutableAclService;

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;
    
    private final String DATASET_CLASS_NAME = DataSet.class.getName();

    /**
     * Deletes data set.
     * <strong>Delete permissions required.</strong>
     *
     * @param providerId identifier of the dataset's provider(required).
     * @param dataSetId  identifier of the deleted data set(required).
     *
     * @return Empty response with status code indicating whether the operation was successful or not.
     * 
     * @throws DataSetNotExistsException data set not exists.
     */
    @DELETE
    @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', delete)")
    public Response deleteDataSet(@PathParam(P_DATASET) String dataSetId, @PathParam(P_PROVIDER) String providerId)
            throws DataSetNotExistsException {
    	
        dataSetService.deleteDataSet(providerId, dataSetId);
        
        // let's delete the permissions as well
        String ownersName = SpringUserUtils.getUsername();
        if (ownersName != null) {
            ObjectIdentity dataSetIdentity = new ObjectIdentityImpl(DATASET_CLASS_NAME,
            		dataSetId + "/" + providerId);
            mutableAclService.deleteAcl(dataSetIdentity, false);
        }
        return Response.ok().build();
    }

    /**
     * Lists representation versions from data set. Result is returned in
     * slices.
     * @summary get representation versions from a data set
     *
     * @param providerId  identifier of the dataset's provider (required).
     * @param dataSetId  identifier of a data set (required).
     * @param startFrom reference to next slice of result. If not provided,
     * first slice of result will be returned.
     * @return slice of representation version list.
     * @throws DataSetNotExistsException no such data set exists.
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<eu.europeana.cloud.common.model.Representation>")
    public ResultSlice<Representation> getDataSetContents(@PathParam(P_DATASET) String dataSetId,
    		@PathParam(P_PROVIDER) String providerId,
    		@QueryParam(F_START_FROM) String startFrom)
            throws DataSetNotExistsException {
        return dataSetService.listDataSet(providerId, dataSetId, startFrom, numberOfElementsOnPage);
    }

    /**
     * Updates description of a data set.
     *
     * <strong>Write permissions required.</strong>
     *
     * @param providerId  identifier of the dataset's provider (required).
     * @param dataSetId  identifier of a data set (required).
     * @param description description of data set
     * @throws DataSetNotExistsException no such data set exists.
     * @throws AccessDeniedOrObjectDoesNotExistException there is an attempt to access a resource without the proper permissions.
     * or the resource does not exist at all
     * @statuscode 204 object has been updated.
     */
    @PUT
    @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)")
    public void updateDataSet(@PathParam(P_DATASET) String dataSetId,
    		@PathParam(P_PROVIDER) String providerId,
    		@FormParam(F_DESCRIPTION) String description)
            throws AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException {
        dataSetService.updateDataSet(providerId, dataSetId, description);
    }
}

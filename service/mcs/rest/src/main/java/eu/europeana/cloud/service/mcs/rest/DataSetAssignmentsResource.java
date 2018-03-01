package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import static eu.europeana.cloud.common.web.ParamConstants.F_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.F_REPRESENTATIONNAME;
import static eu.europeana.cloud.common.web.ParamConstants.F_VER;
import static eu.europeana.cloud.common.web.ParamConstants.P_DATASET;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

/**
 * Resource to assign and unassign representations to/from data sets.
 */
@Path("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}/assignments")
@Component
@Scope("request")
public class DataSetAssignmentsResource {

    @Autowired
    private DataSetService dataSetService;

    /**
     * Assigns representation into a data set.
     * <strong>Write permissions required.</strong>
     *
     *
     * @param providerId identifier of provider(required)
     * @param dataSetId identifier of data set (required)
     * @param recordId cloud id of record (required)
     * @param schema schema of representation (required)
     * @param representationVersion version of representation. If not provided,
     * latest persistent version will be assigned to data set.
     * @throws DataSetNotExistsException no such data set exists
     * @throws RepresentationNotExistsException no such representation exists.
     * @statuscode 204 object assigned.
     */
    @POST
    @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)")
    public void addAssignment(@PathParam(P_PROVIDER) String providerId,
    		@PathParam(P_DATASET) String dataSetId,
    		@FormParam(F_CLOUDID) String recordId, @FormParam(F_REPRESENTATIONNAME) String schema,
            @FormParam(F_VER) String representationVersion)
            throws DataSetNotExistsException, RepresentationNotExistsException {
        ParamUtil.require(F_CLOUDID, recordId);
        ParamUtil.require(F_REPRESENTATIONNAME, schema);
        dataSetService.addAssignment(providerId, dataSetId, recordId, schema, representationVersion);
    }

    /**
     * Unassigns representation from a data set. If representation was not
     * assigned to a data set, nothing happens.
     * <strong>Write permissions required.</strong>
     *
     *@summary Unassign representation from a data set.
     * @param providerId identifier of provider(required)
     * @param dataSetId identifier of data set (required)
     * @param recordId cloud id of record (required)
     * @param schema schema of representation (required)
     * @throws DataSetNotExistsException no such data set exists
     */
    @DELETE
    @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)")
    public void removeAssignment(@PathParam(P_PROVIDER) String providerId,
    		@PathParam(P_DATASET) String dataSetId,
    		@QueryParam(F_CLOUDID) String recordId, @QueryParam(F_REPRESENTATIONNAME) String schema, @QueryParam(F_VER) String versionId)
            throws DataSetNotExistsException {
        ParamUtil.require(F_CLOUDID, recordId);
        ParamUtil.require(F_REPRESENTATIONNAME, schema);
        ParamUtil.require(F_VER, versionId);
        dataSetService.removeAssignment(providerId, dataSetId, recordId, schema, versionId);
    }
}

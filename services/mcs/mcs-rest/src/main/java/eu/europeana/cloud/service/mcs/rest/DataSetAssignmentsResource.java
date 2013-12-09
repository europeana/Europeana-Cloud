package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_GID;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_SCHEMA;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_VER;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_DATASET;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_PROVIDER;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resource to assign and unassign representations to/from data sets.
 */
@Path("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}/assignments")
@Component
public class DataSetAssignmentsResource {

    @PathParam(P_PROVIDER)
    private String providerId;

    @PathParam(P_DATASET)
    private String dataSetId;

    @Autowired
    private DataSetService dataSetService;


    /**
     * Assigns representation into a data set.
     * 
     * @param recordId
     *            cloud id of record (required)
     * @param schema
     *            schema of representation (required)
     * @param representationVersion
     *            version of representation. If not provided, latest persistent version will be assigned to data set.
     * @throws DataSetNotExistsException
     *             no such data set exists
     * @throws RepresentationNotExistsException
     *             no such representation exists.
     * @statuscode 204 object assigned.
     */
    @POST
    public void addAssignment(@FormParam(F_GID) String recordId, @FormParam(F_SCHEMA) String schema,
            @FormParam(F_VER) String representationVersion)
            throws DataSetNotExistsException, RepresentationNotExistsException {
        ParamUtil.require(F_GID, recordId);
        ParamUtil.require(F_SCHEMA, schema);
        dataSetService.addAssignment(providerId, dataSetId, recordId, schema, representationVersion);
    }


    /**
     * Unassigns representation from data set. If representation was not assigned to data set, nothing happens. *
     * 
     * @param recordId
     *            cloud id of record (required)
     * @param schema
     *            schema of representation (required)
     * @throws DataSetNotExistsException
     *             no such data set exists
     */
    @DELETE
    public void removeAssignment(@QueryParam(F_GID) String recordId, @QueryParam(F_SCHEMA) String schema)
            throws DataSetNotExistsException {
        ParamUtil.require(F_GID, recordId);
        ParamUtil.require(F_SCHEMA, schema);
        dataSetService.removeAssignment(providerId, dataSetId, recordId, schema);
    }
}

package eu.europeana.cloud.service.mcs.rest;

import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;
import eu.europeana.cloud.service.mcs.DataSetService;

/**
 * DataSetAssignmentsResource
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


    @POST
    public void addAssignment(
            @FormParam(F_GID) String recordId,
            @FormParam(F_REP) String representationName,
            @FormParam(F_VER) String representationVersion) {
        ParamUtil.require(F_GID, recordId);
        ParamUtil.require(F_REP, representationName);
        dataSetService.addAssignment(providerId, dataSetId, recordId, representationName, representationVersion);
    }


    @DELETE
    public void removeAssignment(
            @QueryParam(F_GID) String recordId,
            @QueryParam(F_REP) String representationName) {
        ParamUtil.require(F_GID, recordId);
        ParamUtil.require(F_REP, representationName);
        dataSetService.removeAssignment(providerId, dataSetId, recordId, representationName);
    }
}

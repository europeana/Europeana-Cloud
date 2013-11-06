package eu.europeana.cloud.service.mcs.rest;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;

/**
 * RepresentationSearchResource
 */
@Path("representations")
@Component
public class RepresentationSearchResource {

    @Autowired
    private RecordService recordService;


    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public List<Representation> searchRepresentations(
            @QueryParam(F_PROVIDER) String providerId,
            @QueryParam(F_DATASET) String dataSetId,
            @QueryParam(F_REP) String representationName) {
        // data set id is meaningful only with connection with provider id
        if (dataSetId != null) {
            ParamUtil.require(F_PROVIDER, providerId);
        }
        // at least one parameter must be provided
        if (providerId == null && dataSetId == null && representationName == null) {
            ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.name(), "At least one parameter must be provided");
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo).build());
        }
        return recordService.search(providerId, representationName, dataSetId);
    }
}

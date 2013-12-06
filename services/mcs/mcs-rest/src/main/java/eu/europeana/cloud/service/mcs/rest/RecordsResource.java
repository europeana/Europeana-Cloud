package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_GID;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;

/**
 * RecordsResource
 */
@Path("/records/{" + P_GID + "}")
@Component
public class RecordsResource {

    private static final Logger log = LoggerFactory.getLogger(RecordsResource.class);

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_GID)
    private String globalId;


    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public Record getRecord()
            throws RecordNotExistsException {
        Record record = recordService.getRecord(globalId);
        prepare(record);
        return record;
    }


    @DELETE
    public Response deleteRecord()
            throws RecordNotExistsException {
        recordService.deleteRecord(globalId);
        return Response.noContent().build();
    }


    /**
     * Removes unimportant (at this point) information from record to reduce response size.
     * 
     * @param record
     */
    private void prepare(Record record) {
        EnrichUriUtil.enrich(uriInfo, record);
        for (Representation representation : record.getRepresentations()) {
            representation.setFiles(null);
            representation.setRecordId(null);
        }
    }
}

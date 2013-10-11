package eu.europeana.cloud.contentserviceapi.rest;

import java.net.URI;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.contentserviceapi.exception.RecordNotExistsException;
import eu.europeana.cloud.contentserviceapi.model.Record;
import eu.europeana.cloud.contentserviceapi.model.Representation;
import eu.europeana.cloud.contentserviceapi.service.RecordService;

/**
 * RecordsResource
 */
@Path("/records/{ID}")
@Component
public class RecordsResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam("ID")
    private String globalId;


    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Record getRecord()
            throws RecordNotExistsException {
        Record record = recordService.getRecord(globalId);
        prepare(record);
        return record;
    }


    @DELETE
    public void deleteRecord()
            throws RecordNotExistsException {
        recordService.deleteRecord(globalId);
    }


    private void prepare(Record record) {
        EnrichUriUtil.enrich(uriInfo, record);
        for (Representation representation : record.getRepresentations()) {
            representation.setFiles(null);
            representation.setRecordId(null);
        }
    }
}

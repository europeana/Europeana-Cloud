package eu.europeana.cloud.service.mcs.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;

/**
 * Resource representing records.
 */
@Path("/records/{" + P_CLOUDID + "}")
@Component
@Scope("request")
public class RecordsResource {

    @Autowired
    private RecordService recordService;

    /**
     * Returns record with all its latest persistent representations.
     *
     * @param globalId cloud id of the record (required).
     * @return record.
     * @throws RecordNotExistsException provided id is not known to Unique
     * Identifier Service.
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("eu.europeana.cloud.common.model.Record")
    public Record getRecord(@Context UriInfo uriInfo, @PathParam(P_CLOUDID) String globalId)
            throws RecordNotExistsException {
        Record record = recordService.getRecord(globalId);
        prepare(uriInfo, record);
        return record;
    }

    /**
     * Deletes record with all its representations in all versions. Does not
     * remove mapping from Unique Identifier Service.
     *
     * <strong>Admin permissions required.</strong>
     *
     * @summary delete a record
     * @param globalId cloud id of the record (required).
     * @throws RecordNotExistsException provided id is not known to Unique
     * Identifier Service.
     * @throws RepresentationNotExistsException thrown if no representation can
     * be found for requested record. Service cannot delete such record.
     */
    @DELETE
    @PreAuthorize("hasRole('ROLE_ADMIN')") 
    public void deleteRecord(@PathParam(P_CLOUDID) String globalId)
            throws RecordNotExistsException, RepresentationNotExistsException {
        recordService.deleteRecord(globalId);
    }

    /**
     * Removes unimportant (at this point) information from record to reduce
     * response size.
     *
     * @param record
     */
    private void prepare(@Context UriInfo uriInfo, Record record) {
        EnrichUriUtil.enrich(uriInfo, record);
        for (Representation representation : record.getRepresentations()) {
//            representation.setFiles(null);
            representation.setCloudId(null);
        }
    }
}

package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;

import java.util.Date;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.RepresentationSearchParams;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;

/**
 * RepresentationSearchResource
 */
@Path("representations")
@Component
public class RepresentationSearchResource {

    @Autowired
    private RecordService recordService;

    // ISO8601 standard
    private static final DateTimeFormatter dateFormat = ISODateTimeFormat.dateOptionalTimeParser();


    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public ResultSlice<Representation> searchRepresentations(@QueryParam(F_PROVIDER) String providerId,
            @QueryParam(F_DATASET) String dataSetId, @QueryParam(F_SCHEMA) String representationName,
            @QueryParam(F_DATE_FROM) String creationDateFrom, @QueryParam(F_DATE_UNTIL) String creationDateUntil,
            @QueryParam(F_PERSISTENT) Boolean persistent, @QueryParam(F_START_FROM) String startFrom) {

        // at least one parameter must be provided
        if (allNull(providerId, dataSetId, representationName, creationDateFrom, creationDateUntil, persistent)) {
            ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.name(), "At least one parameter must be provided");
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo).build());
        }

        // parse creation date from
        Date creationDateFromParsed = null;
        if (creationDateFrom != null) {
            try {
                creationDateFromParsed = dateFormat.parseDateTime(creationDateFrom).toDate();
            } catch (IllegalArgumentException ex) {
                ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.name(), F_DATE_FROM
                        + " paremeter has wrong format: " + ex.getMessage());
                throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo)
                        .build());
            }
        }

        // parse creation date until
        Date creationDateUntilParsed = null;
        if (creationDateUntil != null) {
            try {
                creationDateFromParsed = dateFormat.parseDateTime(creationDateUntil).toDate();
            } catch (IllegalArgumentException ex) {
                ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.name(), F_DATE_UNTIL
                        + " paremeter has wrong format: " + ex.getMessage());
                throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo)
                        .build());
            }
        }

        // create search params object
        RepresentationSearchParams params = RepresentationSearchParams.builder().setDataProvider(providerId)
                .setDataSetId(dataSetId).setFromDate(creationDateFromParsed).setPersistent(persistent)
                .setSchema(representationName).setToDate(creationDateUntilParsed).build();

        // invoke record service method
        return recordService.search(params, startFrom, ParamUtil.numberOfElements());
    }


    /**
     * Returns true if all provided objects are null.
     * 
     * @param objects
     * @return true if all provided parameters are null, false if at least one parameter is not null.
     */
    private boolean allNull(Object... objects) {
        for (Object o : objects) {
            if (o != null) {
                return false;
            }
        }
        return true;
    }
}

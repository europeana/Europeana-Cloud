package eu.europeana.cloud.service.dls.rest;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.dls.RepresentationSearchParams;
import eu.europeana.cloud.service.dls.services.SearchRecordService;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Date;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * This resource allows to search representation versions by multiple parameters.
 */
@Path("representations")
@Component
@Scope("request")
public class RepresentationSearchResource {

    @Autowired
    private SearchRecordService recordService;

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;

    // ISO8601 standard
    private static final DateTimeFormatter DATE_FORMAT = ISODateTimeFormat.dateOptionalTimeParser();

    private static final String errorCode = "OTHER";


    /**
     * Searches for representation versions by multiple parameters. Result is returned in slices which contain fixed
     * amount of results and reference (token) to next slice of results. To obtain next result slice, all search
     * parameters in subsequent requests with consecutive result tokens must be the same as in first request. All search
     * parameters are optional but at least one must be provided.
     * 
     * @param providerId
     *            Identifier of representation version's provider.
     * @param dataSetId
     *            Identifier of data set
     * @param dataSetProviderId
     *            Identivier of data set's provider.
     * @param schema
     *            schema of representation
     * @param creationDateFrom
     *            earliest date of representation version's creation. Must be in ISO8601 format (date with optional
     *            time): yyyy-MM-dd'T'HH:mm:ss.SSSZZ or yyyy-MM-dd
     * @param creationDateUntil
     *            latest date of representation version's creation. Must be in ISO8601 format (date with optional time):
     *            yyyy-MM-dd'T'HH:mm:ss.SSSZZ or yyyy-MM-dd
     * @param persistent
     *            Indicator if representation version must be persistent.
     * @param startFrom
     *            Threshold param to indicate result slice
     * @return found representation versions (in slices).
     * @statuscode 400 If no search parameter is provided.
     * 
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public ResultSlice<Representation> searchRepresentations( //
            @QueryParam(F_PROVIDER) String providerId, //
            @QueryParam(F_DATASET) String dataSetId, //
            @QueryParam(F_DATASET_PROVIDER_ID) String dataSetProviderId, //
            @QueryParam(F_REPRESENTATIONNAME) String schema, //
            @QueryParam(F_DATE_FROM) String creationDateFrom, //
            @QueryParam(F_DATE_UNTIL) String creationDateUntil, //
            @QueryParam(F_PERSISTENT) Boolean persistent, //
            @QueryParam(F_START_FROM) String startFrom) {

        // at least one parameter must be provided
        if (allNull(providerId, dataSetId, dataSetProviderId, schema, creationDateFrom, creationDateUntil, persistent)) {
            ErrorInfo errorInfo = new ErrorInfo(errorCode, "At least one parameter must be provided");
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo).build());
        }

        // parse creation date from
        Date creationDateFromParsed = null;
        if (creationDateFrom != null) {
            creationDateFromParsed = parseDate(creationDateFrom, F_DATE_FROM);
        }

        // parse creation date until
        Date creationDateUntilParsed = null;
        if (creationDateUntil != null) {
            creationDateUntilParsed = parseDate(creationDateUntil, F_DATE_UNTIL);
        }

        // create search params object
        RepresentationSearchParams params = RepresentationSearchParams.builder().setDataProvider(providerId)//
                .setDataSetId(dataSetId) //
                .setDataSetProviderId(dataSetProviderId) //
                .setFromDate(creationDateFromParsed) //
                .setPersistent(persistent) //
                .setSchema(schema) //
                .setToDate(creationDateUntilParsed) //
                .build();

        // invoke record service method
        return recordService.search(params, startFrom, numberOfElementsOnPage);
    }


    /**
     * Parses a date-time from the given text, returning as a Date. Uses
     * {@link ISODateTimeFormat#dateOptionalTimeParser} as a parser.
     * 
     * @param date
     *            a string to parse
     * @param dateParamName
     *            a name of the parameter which hold a date
     * @return the parsed date-time
     * @throws WebApplicationException
     *             if the given date cannot be parsed
     */
    private Date parseDate(String date, String dateParamName) {
        Date dateParsed = null;
        if (date != null) {
            try {
                dateParsed = DATE_FORMAT.parseDateTime(date).toDate();
            } catch (IllegalArgumentException ex) {
                ErrorInfo errorInfo = new ErrorInfo(errorCode, dateParamName + " parameter has wrong format: "
                        + ex.getMessage());
                throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo)
                        .build());
            }
        }
        return dateParsed;
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

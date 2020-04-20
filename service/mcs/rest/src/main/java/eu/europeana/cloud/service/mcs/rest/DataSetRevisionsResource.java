package eu.europeana.cloud.service.mcs.rest;

/**
 * @author akrystian
 */

import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.web.bind.annotation.*;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource to manage data sets.
 */
@RestController
@RequestMapping("/data-providers/{"+PROVIDER_ID+"}/data-sets/{"+DATA_SET_ID+"}/representations/" +
        "{"+REPRESENTATION_NAME+"}/revisions/{"+REVISION_NAME+"}/revisionProvider/{"+REVISION_PROVIDER_ID+"}")
@Scope("request")
public class DataSetRevisionsResource {

    @Autowired
    private DataSetService dataSetService;

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;

    @Autowired
    private MutableAclService mutableAclService;

    /**
     * Lists cloudIds from data set. Result is returned in
     * slices.
     *
     * @param providerId         identifier of the dataset's provider.
     * @param dataSetId          identifier of a data set.
     * @param representationName representation name.
     * @param revisionName       name of the revision
     * @param revisionProviderId provider of revision
     * @param revisionTimestamp  timestamp used for identifying revision, must be in UTC format
     * @param startFrom          reference to next slice of result. If not provided, first slice of result will be returned.
     * @param limit
     * @return slice of cloud id with tags of the revision list.
     * @throws DataSetNotExistsException no such data set exists.
     * @summary get representation versions from a data set
     */
    @GetMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity<ResultSlice<CloudTagsResponse>> getDataSetContents(
            @PathVariable(PROVIDER_ID) String providerId,
            @PathVariable(DATA_SET_ID) String dataSetId,
            @PathVariable(REPRESENTATION_NAME) String representationName,
            @PathVariable(REVISION_NAME) String revisionName,
            @PathVariable(REVISION_PROVIDER_ID) String revisionProviderId,
            @RequestParam String revisionTimestamp,
            @RequestParam(required = false) String startFrom,
            @RequestParam(required = false) int limit) throws DataSetNotExistsException, ProviderNotExistsException {

        // when limitParam is specified we can retrieve more results than configured number of elements per page
        final int limitWithNextSlice = (limit > 0 && limit <= 10000) ? limit : numberOfElementsOnPage;

        DateTime timestamp = new DateTime(revisionTimestamp, DateTimeZone.UTC);

        ResultSlice<CloudTagsResponse> result = dataSetService.getDataSetsRevisions(providerId, dataSetId,
                revisionProviderId, revisionName, timestamp.toDate(), representationName, startFrom, limitWithNextSlice);
        return ResponseEntity.ok(result);
    }
}



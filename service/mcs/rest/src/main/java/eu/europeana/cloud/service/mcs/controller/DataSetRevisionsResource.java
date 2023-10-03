package eu.europeana.cloud.service.mcs.controller;

/**
 * @author akrystian
 */

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.DATA_SET_REVISIONS_RESOURCE;

import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Resource to manage data sets.
 */
@RestController
@RequestMapping(DATA_SET_REVISIONS_RESOURCE)
public class DataSetRevisionsResource {

  private final DataSetService dataSetService;

  @Value("${numberOfElementsOnPage}")
  private int numberOfElementsOnPage;

  public DataSetRevisionsResource(DataSetService dataSetService) {
    this.dataSetService = dataSetService;
  }

  /**
   * Lists cloudIds from data set. Result is returned in slices.
   *
   * @param providerId identifier of the dataset's provider.
   * @param dataSetId identifier of a data set.
   * @param representationName representation name.
   * @param revisionName name of the revision
   * @param revisionProviderId provider of revision
   * @param revisionTimestamp timestamp used for identifying revision, must be in UTC format
   * @param existingOnly if set to true, records with deleted flag would be filtered out, additionally the result would not
   * contain nextToken, so continuation request would be impossible.
   * @param startFrom reference to next slice of result. If not provided, first slice of result will be returned.
   * @param limit
   * @return slice of cloud id with tags of the revision list.
   * @throws DataSetNotExistsException no such data set exists.
   * @summary get representation versions from a data set
   */
  @GetMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<ResultSlice<CloudTagsResponse>> getDataSetContents(
      @PathVariable String providerId,
      @PathVariable String dataSetId,
      @PathVariable String representationName,
      @PathVariable String revisionName,
      @PathVariable String revisionProviderId,
      @RequestParam String revisionTimestamp,
      @RequestParam(defaultValue = "false") boolean existingOnly,
      @RequestParam(required = false) String startFrom,
      @RequestParam int limit) throws DataSetNotExistsException, ProviderNotExistsException {

    if (existingOnly && startFrom != null) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "Could not continue query with parameter existingOnly=true! It is not allowed together with 'startFrom' parameter.");
    }

    // when limitParam is specified we can retrieve more results than configured number of elements per page
    final int limitWithNextSlice = (limit > 0 && limit <= 10000) ? limit : numberOfElementsOnPage;

    DateTime timestamp = new DateTime(revisionTimestamp, DateTimeZone.UTC);

    ResultSlice<CloudTagsResponse> result;
    if (existingOnly) {
      result = new ResultSlice(null, dataSetService.getDataSetsExistingRevisions(providerId, dataSetId,
          revisionProviderId, revisionName, timestamp.toDate(), representationName, limitWithNextSlice));
    } else {
      result = dataSetService.getDataSetsRevisions(providerId, dataSetId,
          revisionProviderId, revisionName, timestamp.toDate(), representationName, startFrom, limitWithNextSlice);
    }

    return ResponseEntity.ok(result);
  }
}



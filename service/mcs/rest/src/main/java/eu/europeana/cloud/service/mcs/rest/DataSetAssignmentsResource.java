package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.DATA_SET_ASSIGNMENTS;

/**
 * Resource to assign and unassign representations to/from data sets.
 */
@RestController
@RequestMapping(DATA_SET_ASSIGNMENTS)
public class DataSetAssignmentsResource {

    private final DataSetService dataSetService;

    public DataSetAssignmentsResource(DataSetService dataSetService) {
        this.dataSetService = dataSetService;
    }

    /**
     * Assigns representation into a data set.
     * <strong>Write permissions required.</strong>
     *
     *
     * @param providerId identifier of provider(required)
     * @param dataSetId identifier of data set (required)
     * @param cloudId cloud id of record (required)
     * @param representationName schema of representation (required)
     * @param version version of representation. If not provided,
     * latest persistent version will be assigned to data set.
     * @throws DataSetNotExistsException no such data set exists
     * @throws RepresentationNotExistsException no such representation exists.
     * @statuscode 204 object assigned.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)")
    public void addAssignment(
            @PathVariable String providerId,
            @PathVariable String dataSetId,
            @RequestParam String cloudId,
            @RequestParam String representationName,
            @RequestParam(required = false) String version)
            throws DataSetNotExistsException, RepresentationNotExistsException {
        dataSetService.addAssignment(providerId, dataSetId, cloudId, representationName, version);
    }

    /**
     * Unassigns representation from a data set. If representation was not
     * assigned to a data set, nothing happens.
     * <strong>Write permissions required.</strong>
     *
     *@summary Unassign representation from a data set.
     * @param providerId identifier of provider(required)
     * @param dataSetId identifier of data set (required)
     * @param cloudId cloud id of record (required)
     * @param representationName schema of representation (required)
     * @throws DataSetNotExistsException no such data set exists
     */
    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)")
    public void removeAssignment(
            @PathVariable String providerId,
            @PathVariable String dataSetId,
    		@RequestParam String cloudId,
            @RequestParam String representationName,
            @RequestParam String version)
            throws DataSetNotExistsException {
        dataSetService.removeAssignment(providerId, dataSetId, cloudId, representationName, version);
    }
}

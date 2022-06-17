package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.UUID;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_RESOURCE;

/**
 * Resource to manage representations.
 */
@RestController
@RequestMapping(REPRESENTATION_RESOURCE)
public class RepresentationResource {

	private final RecordService recordService;

	public RepresentationResource(RecordService recordService) {
		this.recordService = recordService;
	}

	/**
	 * Returns the latest persistent version of a given representation .
	 * <strong>Read permissions required.</strong>
	 *
	 * @summary get a representation
	 * @param cloudId cloud id of the record which contains the representation .
	 * @param representationName name of the representation .
	 *
	 * @return requested representation in its latest persistent version.
	 * @throws RepresentationNotExistsException
	 *             representation does not exist or no persistent version of
	 *             this representation exists.
	 */
	@GetMapping(produces = { MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE })
	@PreAuthorize("isAuthenticated()")
	@ResponseBody
	public Representation getRepresentation(
			HttpServletRequest httpServletRequest,
			@PathVariable String cloudId,
			@PathVariable String representationName) throws RepresentationNotExistsException {

		Representation info = recordService.getRepresentation(cloudId, representationName);
		prepare(httpServletRequest, info);
		return info;
	}

	/**
	 * Deletes representation with all of its versions for a given cloudId.
	 * <strong>Admin permissions required.</strong>
	 * @summary Delete a representation.
	 *
	 * @param cloudId cloud id of the record which all the representations will be deleted (required)
	 * @param representationName name of the representation to be deleted (required)
	 * @throws RepresentationNotExistsException
	 *             Representation does not exist.
	 */
	@DeleteMapping
	@ResponseStatus(HttpStatus.NO_CONTENT)
	@PreAuthorize("hasRole('ROLE_ADMIN')")
	public void deleteRepresentation(
			@PathVariable String cloudId,
			@PathVariable String representationName) throws RepresentationNotExistsException {

		recordService.deleteRepresentation(cloudId, representationName);
	}

	/**
	 * Creates a new representation version. Url of the created representation version
	 * will be returned in response.
	 *
	 * <strong>User permissions required.</strong>
	 *
	 * @summary Creates a new representation version.
	 *
	 * @param cloudId cloud id of the record in which the new representation will be created (required).
	 * @param representationName name of the representation to be created (required).
	 * @param providerId
	 *            provider id of this representation version.
	 * @param dataSetId
	 * 				dataset where newly created representation will be assigned
	 * @return The url of the created representation.
	 * @throws RecordNotExistsException
	 *             provided id is not known to Unique Identifier Service.
	 * @throws ProviderNotExistsException
	 *             no provider with given id exists
	 * @statuscode 201 object has been created.
	 */
	@PostMapping(consumes = {MediaType.APPLICATION_FORM_URLENCODED_VALUE})
	@PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)")
	public ResponseEntity<Void> createRepresentation(
			HttpServletRequest httpServletRequest,
			@PathVariable String cloudId,
			@PathVariable String representationName,
			@RequestParam String providerId,
			@RequestParam String dataSetId,
			@RequestParam(required = false) UUID version
	) throws RecordNotExistsException, ProviderNotExistsException, DataSetAssignmentException, RepresentationNotExistsException, DataSetNotExistsException {

		var representation = recordService.createRepresentation(cloudId, representationName, providerId, version, dataSetId);
		EnrichUriUtil.enrich(httpServletRequest, representation);
		return ResponseEntity.created(representation.getUri()).build();
	}

	private void prepare(HttpServletRequest httpServletRequest, Representation representation) {
		EnrichUriUtil.enrich(httpServletRequest, representation);
	}
}

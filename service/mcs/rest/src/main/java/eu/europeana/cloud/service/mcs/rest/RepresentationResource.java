package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;

import static eu.europeana.cloud.common.web.ParamConstants.CLOUD_ID;
import static eu.europeana.cloud.common.web.ParamConstants.REPRESENTATION_NAME;

/**
 * Resource to manage representations.
 */
@RestController
@RequestMapping("/records/{"+CLOUD_ID+"}/representations/{"+REPRESENTATION_NAME+"}")
@Scope("request")
public class RepresentationResource {
	private final String REPRESENTATION_CLASS_NAME = Representation.class.getName();

	@Autowired
	private RecordService recordService;

	@Autowired
	private MutableAclService mutableAclService;

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
    @PostAuthorize("hasPermission"
    	    + "( "
    	    + " (#globalId).concat('/').concat(#schema).concat('/').concat(returnObject.version) ,"
    	    + " 'eu.europeana.cloud.common.model.Representation', read" + ")")
	public @ResponseBody Representation getRepresentation(
			HttpServletRequest httpServletRequest,
			@PathVariable(CLOUD_ID) String cloudId,
			@PathVariable(REPRESENTATION_NAME) String representationName) throws RepresentationNotExistsException {

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
	@PreAuthorize("hasRole('ROLE_ADMIN')")
	public void deleteRepresentation(
			@PathVariable(CLOUD_ID) String cloudId,
			@PathVariable(REPRESENTATION_NAME) String representationName) throws RepresentationNotExistsException {

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
	 * @return The url of the created representation.
	 * @throws RecordNotExistsException
	 *             provided id is not known to Unique Identifier Service.
	 * @throws ProviderNotExistsException
	 *             no provider with given id exists
	 * @statuscode 201 object has been created.
	 */
	@PostMapping(consumes = {MediaType.APPLICATION_FORM_URLENCODED_VALUE})
	@PreAuthorize("isAuthenticated()")
	public ResponseEntity<?> createRepresentation(
			HttpServletRequest httpServletRequest,
			@PathVariable String cloudId,
			@PathVariable String representationName,
			@RequestParam String providerId) throws RecordNotExistsException, ProviderNotExistsException {

		Representation version = recordService.createRepresentation(cloudId, representationName, providerId);
		EnrichUriUtil.enrich(httpServletRequest, version);

		String creatorName = SpringUserUtils.getUsername();
		if (creatorName != null) {

			ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
					cloudId + "/" + representationName + "/" + version.getVersion());

			MutableAcl versionAcl = mutableAclService
					.createAcl(versionIdentity);

			versionAcl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
			versionAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
			versionAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
			versionAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName), true);

			mutableAclService.updateAcl(versionAcl);
		}

		return ResponseEntity.created(version.getUri()).build();
	}

	private void prepare(HttpServletRequest httpServletRequest, Representation representation) {
		EnrichUriUtil.enrich(httpServletRequest, representation);
	}
}

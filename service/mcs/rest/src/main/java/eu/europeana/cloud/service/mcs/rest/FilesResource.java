package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import eu.europeana.cloud.service.mcs.utils.storage_selector.PreBufferedInputStream;
import eu.europeana.cloud.service.mcs.utils.storage_selector.StorageSelector;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.FILES_RESOURCE;

/**
 * FilesResource
 */
@RestController
@RequestMapping(FILES_RESOURCE)
public class FilesResource {

	private static final Logger LOGGER = LoggerFactory.getLogger(FilesResource.class.getName());
	private final RecordService recordService;
	private final DataSetService dataSetService;
	private final PermissionEvaluator permissionEvaluator;
	private final Integer objectStoreSizeThreshold;

	public FilesResource(
			RecordService recordService,
			DataSetService dataSetService,
			PermissionEvaluator permissionEvaluator,
			Integer objectStoreSizeThreshold) {
		this.recordService = recordService;
		this.dataSetService = dataSetService;
		this.permissionEvaluator = permissionEvaluator;
		this.objectStoreSizeThreshold = objectStoreSizeThreshold;
	}

	/**
	 * Adds a new file to representation version. URI to created resource will
	 * be returned in response as content location. Consumes multipart content -
	 * form data:
	 * <ul>
	 * <li>{@value eu.europeana.cloud.common.web.ParamConstants#F_FILE_MIME} -
	 * file mime type</li>
	 * <li>{@value eu.europeana.cloud.common.web.ParamConstants#F_FILE_NAME} -
	 * file name</li>
	 * <li>{@value eu.europeana.cloud.common.web.ParamConstants#F_FILE_DATA} -
	 * binary stream of file content (required)</li>
	 * </ul>
	 *
	 * <strong>Write permissions required.</strong>
	 * @summary Add a new file to a representation version
	 * @param cloudId cloud id of the record (required).
	 * @param representationName schema of representation (required).
	 * @param version a specific version of the representation(required).
	 * @param mimeType
	 *            mime type of file
	 * @param data
	 *            binary stream of file content (required)
	 * @param fileName
	 *            name of creating file. If fileName does not provided by POST
	 *            request fileName will assigned automatically by service.
	 * @return empty response with tag (content md5) and URI to the created resource
	 *         in content location.
	 * @statuscode 201 object has been created.
	 * @throws RepresentationNotExistsException
	 *             representation does not exist in specified version
	 * @throws CannotModifyPersistentRepresentationException
	 *             specified representation version is persistent and modifying
	 *             its files is not allowed.
	 * @throws FileAlreadyExistsException
	 *             specified file already exist.
	 */
	@PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
	public ResponseEntity<URI> sendFile(
			HttpServletRequest httpServletRequest,
			@PathVariable final String cloudId,
			@PathVariable final String representationName,
			@PathVariable final String version,
			@RequestParam String mimeType,
			@RequestParam MultipartFile data,
			@RequestParam(required = false) String fileName) throws RepresentationNotExistsException,
			CannotModifyPersistentRepresentationException, FileAlreadyExistsException, IOException, AccessDeniedOrObjectDoesNotExistException {

		Representation representation = Representation.fromFields(cloudId, representationName, version);
		if (isUserAllowedToUploadFileFor(representation)) {
			File f = new File();
			f.setMimeType(mimeType);
			PreBufferedInputStream prebufferedInputStream = new PreBufferedInputStream(data.getInputStream(), objectStoreSizeThreshold);
			f.setFileStorage(new StorageSelector(prebufferedInputStream, mimeType).selectStorage());
			if (fileName != null) {
				try {
					File temp = recordService.getFile(cloudId, representationName, version, fileName);
					if (temp != null) {
						throw new FileAlreadyExistsException(fileName);
					}
				} catch (FileNotExistsException e) {
					// file does not exist, so continue and add it
				}
			}

			if (fileName == null) {
				fileName = UUID.randomUUID().toString();
			}
			f.setFileName(fileName);

			recordService.putContent(cloudId, representationName, version, f, prebufferedInputStream);
			IOUtils.closeQuietly(prebufferedInputStream);
			EnrichUriUtil.enrich(httpServletRequest, cloudId, representationName, version, f);

			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("File added [%s, %s, %s], uri: %s ",
						cloudId, representationName, version, f.getContentUri()));
			}

			ResponseEntity.BodyBuilder response = ResponseEntity.created(f.getContentUri());
			if (f.getMd5() != null) {
				response.eTag(f.getMd5());
			}
			return response.build();
		} else {
			throw new AccessDeniedOrObjectDoesNotExistException();
		}
	}

	private boolean isUserAllowedToUploadFileFor(Representation representation) throws RepresentationNotExistsException {
		List<CompoundDataSetId> representationDataSets = dataSetService.getAllDatasetsForRepresentationVersion(representation);
		if (representationDataSets.size() != 1) {
			LOGGER.error("Should never happen");
		} else {
			SecurityContext ctx = SecurityContextHolder.getContext();
			Authentication authentication = ctx.getAuthentication();
			//
			String targetId = representationDataSets.get(0).getDataSetId() + "/" + representationDataSets.get(0).getDataSetProviderId();
			return permissionEvaluator.hasPermission(authentication, targetId, DataSet.class.getName(), "read");
		}
		return false;
	}

}

package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.utils.LogMessageCleaner;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import eu.europeana.cloud.service.mcs.utils.storage_selector.PreBufferedInputStream;
import eu.europeana.cloud.service.mcs.utils.storage_selector.StorageSelector;
import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.FILE_UPLOAD_RESOURCE;

/**
 * Handles uploading the file when representation is not created yet.
 */
@RestController
@RequestMapping(FILE_UPLOAD_RESOURCE)
public class FileUploadResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadResource.class);
  private final RecordService recordService;
  private final Integer objectStoreSizeThreshold;

  public FileUploadResource(
      RecordService recordService,
      Integer objectStoreSizeThreshold) {
    this.recordService = recordService;
    this.objectStoreSizeThreshold = objectStoreSizeThreshold;
  }

  /**
   * Creates representation, uploads file and persists this representation in one request
   *
   * @param cloudId cloudId
   * @param representationName representation name
   * @param fileName file name
   * @param providerId providerId
   * @param mimeType mimeType of uploaded file
   * @param data uploaded file content
   * @return result of the operation. Usually it is OK containing created file location but maybe also exception.
   * @throws RepresentationNotExistsException
   * @throws CannotModifyPersistentRepresentationException
   * @throws RecordNotExistsException
   * @throws ProviderNotExistsException
   * @throws CannotPersistEmptyRepresentationException
   * @summary Upload file for non-existing representation
   */
  @Timed(value = "representation_creation_and_upload_duration", description = "Duration of representation creation and file upload operations", histogram = true)
  @Counted(value = "representation_creation_and_file_upload_counter", description = "Number of file upload operations")
  @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  @PreAuthorize("hasRole('ROLE_EXECUTOR') OR  hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)")
  public ResponseEntity<?> sendFile(
      HttpServletRequest httpServletRequest,
      @PathVariable("cloudId") String cloudId,
      @PathVariable("representationName") String representationName,
      @RequestParam(value = "version", required = false) UUID version,
      @RequestParam("fileName") String fileName,
      @RequestParam("providerId") String providerId,
      @RequestParam("mimeType") String mimeType,
      @RequestParam("dataSetId") String dataSetId,
      @RequestParam("data") MultipartFile data) throws RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, RecordNotExistsException,
      ProviderNotExistsException, CannotPersistEmptyRepresentationException, IOException, DataSetAssignmentException, DataSetNotExistsException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Uploading file cloudId={}, representationName={}, version={}, fileName={}, providerId={}, mime={}",
          LogMessageCleaner.clean(cloudId),
          LogMessageCleaner.clean(representationName),
          version,
          LogMessageCleaner.clean(fileName),
          LogMessageCleaner.clean(providerId),
          LogMessageCleaner.clean(mimeType));
    }
    PreBufferedInputStream prebufferedInputStream = new PreBufferedInputStream(data.getInputStream(), objectStoreSizeThreshold);
    Storage storage = new StorageSelector(prebufferedInputStream, mimeType).selectStorage();
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("File {} buffered",
          LogMessageCleaner.clean(fileName));
    }
    Representation representation = recordService.createRepresentation(cloudId, representationName, providerId, version,
        dataSetId);

    File file = addFileToRepresentation(representation, prebufferedInputStream, mimeType, fileName, storage);
    persistRepresentation(representation);

    EnrichUriUtil.enrich(httpServletRequest, cloudId, representationName, representation.getVersion(), file);

    return ResponseEntity
        .created(file.getContentUri())
        .eTag(file.getMd5())
        .build();
  }

  private File addFileToRepresentation(
      Representation representation, InputStream data, String mimeType, String fileName, Storage storage)
      throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

    File f = new File();
    f.setMimeType(mimeType);
    f.setFileStorage(storage);

    if (fileName == null) {
      fileName = UUID.randomUUID().toString();
    }
    f.setFileName(fileName);
    recordService.putContent(representation.getCloudId(), representation.getRepresentationName(),
        representation.getVersion(), f, data);
    IOUtils.closeQuietly(data);
    return f;
  }

  private void persistRepresentation(Representation representation) throws
      CannotModifyPersistentRepresentationException,
      CannotPersistEmptyRepresentationException,
      RepresentationNotExistsException {
    recordService.persistRepresentation(representation.getCloudId(), representation.getRepresentationName(),
        representation.getVersion());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Representation persisted: {}",
          LogMessageCleaner.clean(representation));
    }
  }
}

package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.utils.FileUtils;
import eu.europeana.cloud.common.utils.LogMessageCleaner;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import eu.europeana.cloud.service.mcs.persistent.s3.PutResult;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.function.Consumer;

/**
 * Implementation of record service using Cassandra as storage.
 */
public class CassandraRecordService implements RecordService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRecordService.class);
  private final CassandraRecordDAO recordDAO;
  private final DataSetService dataSetService;
  private final CassandraDataSetDAO dataSetDAO;
  private final DynamicContentProxy contentDAO;
  private final UISClientHandler uis;

  public CassandraRecordService(CassandraRecordDAO recordDAO, DataSetService dataSetService, CassandraDataSetDAO dataSetDAO,
      DynamicContentProxy contentDAO, UISClientHandler uis) {
    this.recordDAO = recordDAO;
    this.dataSetService = dataSetService;
    this.dataSetDAO = dataSetDAO;
    this.contentDAO = contentDAO;
    this.uis = uis;
  }

  private static void sortByProviderId(List<Representation> input) {
    Collections.sort(input, new Comparator<Representation>() {

      @Override
      public int compare(Representation r1, Representation r2) {
        return r1.getDataProvider().compareToIgnoreCase(r2.getDataProvider());
      }
    });
  }

  private static UUID generateTimeUUID() {
    return UUID.fromString(new com.eaio.uuid.UUID().toString());
  }

  /**
   * @inheritDoc
   */
  @Override
  public Record getRecord(String cloudId) throws RecordNotExistsException {
    Record aRecord = null;
    if (uis.existsCloudId(cloudId)) {
      aRecord = recordDAO.getRecord(cloudId);
      if (aRecord.getRepresentations().isEmpty()) {
        throw new RecordNotExistsException(cloudId);
      }
    } else {
      throw new RecordNotExistsException(cloudId);
    }
    return aRecord;
  }

  /**
   * @inheritDoc
   */
  @Override
  public void deleteRecord(String cloudId) throws RecordNotExistsException, RepresentationNotExistsException {

    if (uis.existsCloudId(cloudId)) {
      List<Representation> allRecordRepresentationsInAllVersions = recordDAO.listRepresentationVersions(cloudId);
      if (allRecordRepresentationsInAllVersions.isEmpty()) {
        throw new RepresentationNotExistsException(String.format(
            "No representation found for given cloudId %s", cloudId));
      }

      sortByProviderId(allRecordRepresentationsInAllVersions);

      for (Representation repVersion : allRecordRepresentationsInAllVersions) {
        removeFilesFromRepresentationVersion(cloudId, repVersion);
        removeRepresentationAssignmentFromDataSets(cloudId, repVersion);
        recordDAO.deleteRepresentation(cloudId, repVersion.getRepresentationName(), repVersion.getVersion());
      }
    } else {
      throw new RecordNotExistsException(cloudId);
    }
  }

  /**
   * @inheritDoc
   */
  @Override
  public void deleteRepresentation(String globalId, String schema) throws RepresentationNotExistsException {

    List<Representation> listRepresentations = recordDAO.listRepresentationVersions(globalId, schema);
    if (listRepresentations == null) {
      throw new RepresentationNotExistsException();
    }

    sortByProviderId(listRepresentations);

    for (Representation rep : listRepresentations) {
      removeFilesFromRepresentationVersion(globalId, rep);
      removeRepresentationAssignmentFromDataSets(globalId, rep);
    }
    recordDAO.deleteRepresentation(globalId, schema);
  }

  @Override
  public Representation createRepresentation(String globalId, String schema, String providerId, String dataSetId, boolean markDepublished)
      throws RecordNotExistsException, ProviderNotExistsException, DataSetAssignmentException,
      RepresentationNotExistsException, DataSetNotExistsException {

      return createRepresentation(globalId, schema, providerId, null, dataSetId, markDepublished);
  }

  @Override
  public Representation createRepresentation(String cloudId, String representationName, String providerId, UUID version,
                                             String dataSetId) throws RecordNotExistsException, ProviderNotExistsException, DataSetAssignmentException,
          RepresentationNotExistsException, DataSetNotExistsException{
    return createRepresentation(cloudId, representationName, providerId, version, dataSetId, false);
  }

  /**
   * @inheritDoc
   */
  @Override
  public Representation createRepresentation(String cloudId, String representationName, String providerId, UUID version,
                                             String dataSetId, boolean markDepublished)
      throws ProviderNotExistsException, RecordNotExistsException, DataSetAssignmentException,
      RepresentationNotExistsException, DataSetNotExistsException {

    checkIfDatasetExists(dataSetId, providerId);

    // check if data provider exists
    if (uis.getProvider(providerId) == null) {
      throw new ProviderNotExistsException(String.format("Provider %s does not exist.", providerId));
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Confirmed provider, id={} exists.",
          LogMessageCleaner.clean(providerId)
      );
    }

    if (version == null) {
      version = generateTimeUUID();
    }else {
      checkIfVersionIsContainedOnlyOneDataset(cloudId, representationName, providerId, version, dataSetId);
    }

    boolean cloudExists = uis.existsCloudId(cloudId);
    if (cloudExists) {
      LOGGER.debug("Confirmed cloudId={} exists.", cloudId);
      Date now = Calendar.getInstance().getTime();
      Representation representation =
              recordDAO.createRepresentation(cloudId, representationName, providerId, now, version, dataSetId, markDepublished);
      dataSetService.addAssignmentToMainTables(
          providerId,
          dataSetId,
          representation.getCloudId(),
          representation.getRepresentationName(),
          representation.getVersion(),
              markDepublished);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Created representation cloudid={}, representationName={}, providerId={}, version={}",
            LogMessageCleaner.clean(cloudId),
            LogMessageCleaner.clean(representationName),
            LogMessageCleaner.clean(providerId),
            version);
      }
      return representation;
    } else {
      throw new RecordNotExistsException(cloudId);
    }
  }

  private void checkIfVersionIsContainedOnlyOneDataset(String cloudId, String representationName, String providerId, UUID version, String dataSetId) throws RepresentationNotExistsException, DataSetAssignmentException {
    Optional<CompoundDataSetId> datasetFor = recordDAO.getRepresentationDatasetId(cloudId, representationName, version);
    if (datasetFor.isPresent()) {
      CompoundDataSetId currentDataset = datasetFor.get();
      CompoundDataSetId newVersionDataset = new CompoundDataSetId(providerId, Set.of(dataSetId));
      if (!currentDataset.equals(newVersionDataset)) {
        throw new DataSetAssignmentException("ProviderId and/or datasetId: " + newVersionDataset
                + " doesn't match the current assignments of the representation: " + currentDataset
                + ". It is not allowed to assign the same representation version of the record to more than one dataset.");
      }
    }
  }

  /**
   * @inheritDoc
   */
  @Override
  public Representation getRepresentation(String globalId, String schema)
      throws RepresentationNotExistsException {
    Representation rep = recordDAO.getLatestPersistentRepresentation(globalId, schema);
    if (rep == null) {
      throw new RepresentationNotExistsException();
    }
    return rep;
  }

  /**
   * @inheritDoc
   */
  @Override
  public Representation getRepresentation(String globalId, String schema, String version)
      throws RepresentationNotExistsException {
    Representation rep = recordDAO.getRepresentation(globalId, schema, version);
    if (rep == null) {
      throw new RepresentationNotExistsException();
    }

    LOGGER.debug("Loaded representation {}", rep);
    return rep;
  }

  /**
   * @inheritDoc
   */
  @Override
  public void deleteRepresentation(String globalId, String schema, String version)
      throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
    Representation rep = recordDAO.getRepresentation(globalId, schema, version);
    if (rep == null) {
      throw new RepresentationNotExistsException();
    }

    removeFilesFromRepresentationVersion(globalId, rep);
    removeRepresentationAssignmentFromDataSets(globalId, rep);
    recordDAO.deleteRepresentation(globalId, schema, version);

  }

  /**
   * @inheritDoc
   */
  @Override
  public Representation persistRepresentation(String globalId, String schema, String version)
      throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
      CannotPersistEmptyRepresentationException {
    Representation rep = recordDAO.getRepresentation(globalId, schema, version);
    if (rep == null) {
      throw new RepresentationNotExistsException();
    } else if (rep.isPersistent()) {
      throw new CannotModifyPersistentRepresentationException();
    }
    List<File> recordFiles = recordDAO.getFilesForRepresentation(globalId, schema, version);
    if (recordFiles == null) {
      throw new RepresentationNotExistsException();

    } else if (recordFiles.isEmpty()) {
      throw new CannotPersistEmptyRepresentationException();
    }

    Date now = Calendar.getInstance().getTime();
    recordDAO.persistRepresentation(globalId, schema, version, now);

    rep.setPersistent(true);
    rep.setCreationDate(now);

    return rep;
  }

  /**
   * @inheritDoc
   */
  @Override
  public List<Representation> listRepresentationVersions(String globalId, String schema)
      throws RepresentationNotExistsException {

    List<Representation> result = recordDAO.listRepresentationVersions(globalId, schema);
    if (result == null) {
      throw new RepresentationNotExistsException();
    }
    return result;
  }

  /**
   * @inheritDoc
   */
  @Override
  public boolean putContent(String globalId, String schema, String version, File file, InputStream content)
      throws CannotModifyPersistentRepresentationException, RepresentationNotExistsException {
    Representation representation = getRepresentation(globalId, schema, version);
    if (representation.isPersistent()) {
      throw new CannotModifyPersistentRepresentationException();
    }
    boolean isCreate = true; // if it is create file operation or update
    // content
    for (File f : representation.getFiles()) {
      if (f.getFileName().equals(file.getFileName())) {
        isCreate = false;
        break;
      }
    }
    String keyForFile = FileUtils.generateKeyForFile(globalId, schema, version, file.getFileName());
    PutResult result;
    try {
      result = contentDAO.putContent(keyForFile, content, file.getFileStorage());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Stored content for file: {} version: {}",
            LogMessageCleaner.clean(file.getFileName()),
            LogMessageCleaner.clean(version));
      }
    } catch (IOException ex) {
      throw new SystemException(ex);
    }

    DateTime now = new DateTime();
    file.setMd5(result.getMd5());
    DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
    file.setDate(fmt.print(now));
    file.setContentLength(result.getContentLength());
    recordDAO.addOrReplaceFileInRepresentation(globalId, schema, version, file);
    LOGGER.debug("Updated file information in representation: {}", representation);
    return isCreate;
  }

  /**
   * @return
   * @inheritDoc
   */
  @Override
  public Consumer<OutputStream> getContent(String globalId, String schema, String version, String fileName, long rangeStart,
      long rangeEnd)
      throws FileNotExistsException, WrongContentRangeException, RepresentationNotExistsException {
    File file = getFile(globalId, schema, version, fileName);
    if (rangeStart > file.getContentLength() - 1) {
      throw new WrongContentRangeException("Start range must be less than file length");
    }

    if (rangeEnd > file.getContentLength() - 1) {
      throw new WrongContentRangeException("End range must be less than file length");
    }
    return os -> {
      try {
        contentDAO.getContent(file.getMd5(), FileUtils.generateKeyForFile(globalId, schema, version, fileName), rangeStart,
            rangeEnd, os, file.getFileStorage());
      } catch (FileNotExistsException | IOException ex) {
        throw new SystemException(ex);
      }
    };

  }

  /**
   * @inheritDoc
   */
  @Override
  public String getContent(String globalId, String schema, String version, String fileName, OutputStream os)
      throws FileNotExistsException, RepresentationNotExistsException {
    Representation rep = getRepresentation(globalId, schema, version);
    File file = findFileInRepresentation(rep, fileName);
    try {
      contentDAO.getContent(file.getMd5(), FileUtils.generateKeyForFile(globalId, schema, version, fileName), -1, -1, os,
          file.getFileStorage());
    } catch (IOException ex) {
      throw new SystemException(ex);
    }
    return file.getMd5();
  }

  /**
   * @inheritDoc
   */
  @Override
  public void deleteContent(String globalId, String schema, String version, String fileName)
      throws FileNotExistsException, CannotModifyPersistentRepresentationException,
      RepresentationNotExistsException {
    Representation representation = getRepresentation(globalId, schema, version);
    if (representation.isPersistent()) {
      throw new CannotModifyPersistentRepresentationException();
    }
    recordDAO.removeFileFromRepresentation(globalId, schema, version, fileName);
    File file = findFileInRepresentation(representation, fileName);
    contentDAO.deleteContent(file.getMd5(), FileUtils.generateKeyForFile(globalId, schema, version, fileName), file.getFileStorage());
  }

  /**
   * @inheritDoc
   */
  @Override
  public File getFile(String globalId, String schema, String version, String fileName)
      throws RepresentationNotExistsException, FileNotExistsException {
    final Representation rep = getRepresentation(globalId, schema, version);
    return findFileInRepresentation(rep, fileName);
  }

  @Override
  public void addAnnotationToRepresentationVersion(Representation representation, RepresentationVersionAnnotation annotation) throws RepresentationNotExistsException {
    LOGGER.debug("adding annotation to representation version");
    Representation rep = recordDAO.getRepresentation(
        representation.getCloudId(),
        representation.getRepresentationName(),
        representation.getVersion());

    if (rep != null) {
      recordDAO.addAnnotationToRepresentation(representation, annotation);
    } else {
      throw new RepresentationNotExistsException(String.format("No representation found for given cloudId %s",
          representation.getCloudId()));
    }
  }

  private void removeFilesFromRepresentationVersion(String cloudId, Representation repVersion) {

    for (File f : repVersion.getFiles()) {
      try {
        contentDAO.deleteContent(f.getMd5(), FileUtils.generateKeyForFile(cloudId, repVersion.getRepresentationName(),
            repVersion.getVersion(), f.getFileName()), f.getFileStorage());
      } catch (FileNotExistsException ex) {
        LOGGER.warn(
            "File '{}' was found in representation {}-{}-{} but no content of such file was found",
            f.getFileName(), cloudId, repVersion.getRepresentationName(), repVersion.getVersion());
      }
    }
  }

  private void checkIfDatasetExists(String dataSetId, String providerId) throws DataSetNotExistsException {
    DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);
    if (ds == null) {
      throw new DataSetNotExistsException();
    }
  }

  private void removeRepresentationAssignmentFromDataSets(String globalId, Representation representation) {
    for (String datasetId : representation.getDatasetIds()) {
        try {
          dataSetService.removeAssignment(
              representation.getDataProvider(),
                  datasetId,
              globalId,
              representation.getRepresentationName(),
              representation.getVersion()
          );
        } catch (DataSetNotExistsException e) {
          //Nothing to do, skip exception
        }
    }
  }

  private File findFileInRepresentation(Representation representation, String fileName) throws FileNotExistsException {
    for (File file : representation.getFiles()) {
      if (file.getFileName().equals(fileName)) {
        return file;
      }
    }
    throw new FileNotExistsException();
  }

}

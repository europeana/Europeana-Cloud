package eu.europeana.cloud.service.mcs;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAssignmentException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionIsNotValidException;
import eu.europeana.cloud.service.mcs.exception.RevisionNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Service for manipulating representations and their content.
 */
public interface RecordService {

  /**
   * Returns latest persistent versions of all representations of the record. The returned record would not contain any trace of
   * representations that do not contain any persistent version. Particularly, if there is no representation for this record or
   * threre are only temporary representations, the returned record will be empty (but not null).
   *
   * @param globalId id for the record.
   * @return record containing all latest persistent versions of all its representations.
   * @throws RecordNotExistsException thrown if provided id is not registered id in the eCloud system.
   */
  Record getRecord(String globalId)
      throws RecordNotExistsException;


  /**
   * Deletes record with all its representations (even those not returned by {@link #getRecord(java.lang.String) getRecord}), both
   * persistent and temporary.
   *
   * @param globalId id for the record
   * @throws RecordNotExistsException thrown if provided id is not registered id in the eCloud system.
   * @throws RepresentationNotExistsException thrown if no representation can be found for requested record. Service cannot delete
   * such record.
   */
  void deleteRecord(String globalId)
      throws RecordNotExistsException, RepresentationNotExistsException;


  /**
   * Returns latest persistent version of particular representation of a record.
   *
   * @param globalId id for the record
   * @param schema schema of representation
   * @return latest persistent version of representation in specified schema
   * @throws RepresentationNotExistsException thrown if there is no representation for provided record and schema or such
   * representation exists but is not persistent.
   */
  Representation getRepresentation(String globalId, String schema)
      throws RepresentationNotExistsException;


  /**
   * Returns a representation of a record in specified schema and version (might be persistent or not).
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @param version version of the representation.
   * @return representation.
   * @throws RepresentationNotExistsException such representation does not exist in specified version.
   */
  Representation getRepresentation(String globalId, String schema, String version)
      throws RepresentationNotExistsException;


  /**
   * Lists all (persistent or not) versions of a record representation.
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @return list of representation versions.
   * @throws RepresentationNotExistsException there is no representation in provided schema for specified record.
   */
  List<Representation> listRepresentationVersions(String globalId, String schema)
      throws RepresentationNotExistsException;


  /**
   * Deletes representation of a record in specific schema with all versions (temporary and persistent).
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @throws RepresentationNotExistsException there is no representation in provided schema for specified record.
   */
  void deleteRepresentation(String globalId, String schema)
      throws RepresentationNotExistsException;


  /**
   * Deletes a specified representation version of a record. Only temporary representations can be removed using this method!
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @param version version of the representation
   * @throws RepresentationNotExistsException such representation does not exist in specified version.
   * @throws CannotModifyPersistentRepresentationException specified representation version is persistent and cannot deleted.
   */
  void deleteRepresentation(String globalId, String schema, String version)
      throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException;


    /**
   * Creates a new representation version of a record. A version of newly created representation might be obtained from returned
   * representation object.
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @param providerId provider who created this representation version.
   * @param version version id - must conform version 1 UUID spec. Parameter is optional if null is passed new uuid based on
   * @param markDeleted whether representation is marked as already deleted
   * current time would be created.
   * @return newly created representation.
   * @throws RecordNotExistsException provided id of a record is not registered in eCloud system.
   * @throws ProviderNotExistsException there is no such provider
   */
  Representation createRepresentation(String globalId, String schema, String providerId, UUID version, String dataSetId, boolean markDeleted)
      throws RecordNotExistsException, ProviderNotExistsException, DataSetAssignmentException,
      RepresentationNotExistsException, DataSetNotExistsException;

  Representation createRepresentation(String globalId, String schema, String providerId, String dataSetId, boolean markDeleted)
      throws RecordNotExistsException, ProviderNotExistsException, DataSetAssignmentException,
      RepresentationNotExistsException, DataSetNotExistsException;

  Representation createRepresentation(String cloudId, String representationName, String providerId, UUID version,
                                      String dataSetId) throws RecordNotExistsException, ProviderNotExistsException, DataSetAssignmentException,
          RepresentationNotExistsException, DataSetNotExistsException;

  /**
   * Makes a certain temporary representation version a persistent one. A representation version must contain files in order to be
   * made persistent.
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @param version version of the representation
   * @return a persisted representation.
   * @throws RepresentationNotExistsException such representation does not exist in specified version.
   * @throws CannotModifyPersistentRepresentationException the representation version is already persistent.
   * @throws CannotPersistEmptyRepresentationException the representation version does not contain files.
   */
  Representation persistRepresentation(String globalId, String schema, String version)
      throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
      CannotPersistEmptyRepresentationException;

  /**
   * Creates or overrides file with specific name in specified representation version.
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @param version version of the representation
   * @param file eCloud file representing content metadata. This object MUST contain fileName and SHOULD contain mime type.
   * @param content stream of file content.
   * @return true if new file was added, false if file already existed in representation version and only its content was updated.
   * @throws RepresentationNotExistsException representation does not exist in specified version.
   * @throws CannotModifyPersistentRepresentationException specified representation version is persistent so its files cannot be
   * modified.
   */
  boolean putContent(String globalId, String schema, String version, File file, InputStream content)
      throws CannotModifyPersistentRepresentationException, RepresentationNotExistsException;

  /**
   * Copy a range of the file content to the output stream. File is identified by global identifier, schema, version number and
   * file name. Range start and range end starts at zero.
   *
   * @param globalId record identifier
   * @param schema record schema
   * @param version version number
   * @param fileName file name
   * @param rangeStart initial index of the range, inclusive
   * @param rangeEnd final index of the range, inclusive.
   * @return
   * @throws RepresentationNotExistsException representation does not exist in specified version.
   * @throws FileNotExistsException if file with given name does not exist
   * @throws WrongContentRangeException if range is invalid
   */
  Consumer<OutputStream> getContent(String globalId, String schema, String version, String fileName, long rangeStart,
      long rangeEnd)
      throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException;


  /**
   * Gets file metadata from a specified representation version.
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @param version version of the representation
   * @param fileName name of requested file.
   * @return File metadata
   * @throws RepresentationNotExistsException representation does not exist in specified version.
   * @throws FileNotExistsException if file with given name does not exist in representation version
   */
  File getFile(String globalId, String schema, String version, String fileName)
      throws RepresentationNotExistsException, FileNotExistsException;


  /**
   * Fetches content of specified file in specified representation version and writes it into output stream.
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @param version version of the representation
   * @param fileName name of requested file.
   * @param os stream which the content will be written into.
   * @return md5 of content
   * @throws RepresentationNotExistsException representation does not exist in specified version.
   * @throws FileNotExistsException if file with given name does not exist in representation version
   */
  String getContent(String globalId, String schema, String version, String fileName, OutputStream os)
      throws RepresentationNotExistsException, FileNotExistsException;


  /**
   * Deletes specified file from temporary representation version.
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @param version version of the representation
   * @param fileName name of file to be removed
   * @throws RepresentationNotExistsException representation does not exist in specified version.
   * @throws FileNotExistsException if file with given name does not exist in representation version
   * @throws CannotModifyPersistentRepresentationException specified representation version is persistent.
   */
  void deleteContent(String globalId, String schema, String version, String fileName)
      throws RepresentationNotExistsException, FileNotExistsException,
      CannotModifyPersistentRepresentationException;


  /**
   * Gets Revision  from a specified representation version.
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @param version version of the representation
   * @param revisionKey name of revision key providerId_revisionName.
   * @return Revision attributes
   * @throws RepresentationNotExistsException representation does not exist in specified version.
   * @throws RevisionNotExistsException if revision with given key does not exist in representation version
   */
  Revision getRevision(String globalId, String schema, String version, String revisionKey)
      throws RepresentationNotExistsException, RevisionNotExistsException;


  /**
   * Creates or overrides revision with specific name.
   *
   * @param globalId id of the record
   * @param schema schema of the representation
   * @param version version of the representation
   * @param revision eCloud revision. This object MUST contain providerId and revisionName type.
   */
  void addRevision(String globalId, String schema, String version, Revision revision) throws RevisionIsNotValidException;


  /**
   * Get RepresentationRevisionResponse object basing on cloud identifier, schema identifier and revision identifier.
   *
   * @param globalId cloud identifier associated with the resulting representation revision
   * @param schema representation name of the resulting representation revision
   * @param revisionProviderId revision provider identifier of the resulting representation revision
   * @param revisionName revision name
   * @param revisionTimestamp revision timestamp
   * @return RepresentationRevisionResponse object that associates cloud identifier, representation name, revision identifier,
   * version identifier and files map
   */
  List<RepresentationRevisionResponse> getRepresentationRevisions(String globalId, String schema, String revisionProviderId,
      String revisionName, Date revisionTimestamp);

  /**
   * Insert information about representation revision association.
   *
   * @param globalId cloud identifier
   * @param schema representation name
   * @param revisionProviderId revision provider identifier of the resulting representation revision
   * @param revisionName revision name
   * @param revisionTimestamp revision timestamp
   * @param versionId version identifier
   */
  void insertRepresentationRevision(String globalId, String schema, String revisionProviderId, String revisionName,
      String versionId, Date revisionTimestamp);
}

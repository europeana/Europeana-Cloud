package eu.europeana.cloud.service.mcs;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Service for manipulating representations and their content.
 */
public interface RecordService {

    Record getRecord(String globalId)
            throws RecordNotExistsException;


    void deleteRecord(String globalId)
            throws RecordNotExistsException;


    //==

    Representation getRepresentation(String globalId, String schema)
            throws RepresentationNotExistsException;


    Representation getRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException;


    List<Representation> listRepresentationVersions(String globalId, String schema)
            throws RepresentationNotExistsException;


    void deleteRepresentation(String globalId, String schema)
            throws RepresentationNotExistsException;


    void deleteRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException;


    Representation createRepresentation(String globalId, String schema, String providerId)
            throws RecordNotExistsException, ProviderNotExistsException;


    //==
    Representation persistRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            CannotPersistEmptyRepresentationException;


    Representation copyRepresentation(String globalId, String schema, String version)
            throws RecordNotExistsException, RepresentationNotExistsException;


    boolean putContent(String globalId, String schema, String version, File file, InputStream content)
            throws RepresentationNotExistsException, FileAlreadyExistsException,
            CannotModifyPersistentRepresentationException;


    /**
     * Copy a range of the file content to the output stream. File is identified by global identifier, schema,
     * version number and file name. Range start and range end starts at zero. 
     * 
     * @param globalId record identifier
     * @param schema record schema
     * @param version version number
     * @param fileName file name
     * @param rangeStart initial index of the range, inclusive 
     * @param rangeEnd final index of the range, inclusive
     * @param outputStream output stream
     * @throws RepresentationNotExistsException if representation for given record identifier and schema identifier does not exist
     * @throws FileNotExistsException if file with given name does not exist
     * @throws WrongContentRangeException if range is invalid
     */
    void getContent(String globalId, String schema, String version, String fileName, long rangeStart, long rangeEnd,
            OutputStream outputStream)
            throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException;

	File getFile(String globalId, String schema, String version, String fileName)
			throws
			RepresentationNotExistsException, FileNotExistsException;
	
	String getContent(String globalId, String schema, String version, String fileName, OutputStream os)
	        throws RepresentationNotExistsException, FileNotExistsException;

    void deleteContent(String globalId, String schema, String version, String fileName)
            throws RepresentationNotExistsException, FileNotExistsException,
            CannotModifyPersistentRepresentationException;


    ResultSlice<Representation> search(String providerId, String representationName, String dataSetId,
            String thresholdParam, int limit);
}

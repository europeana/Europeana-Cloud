package eu.europeana.cloud.service.mcs;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

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
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException;


	Representation copyRepresentation(String globalId, String schema, String version)
			throws RecordNotExistsException, RepresentationNotExistsException;


	boolean putContent(String globalId, String schema, String version, File file, InputStream content)
			throws RepresentationNotExistsException, FileAlreadyExistsException, CannotModifyPersistentRepresentationException;


	void getContent(String globalId, String schema, String version, String fileName, long rangeStart, long rangeEnd, OutputStream os)
			throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException;


	String getContent(String globalId, String schema, String version, String fileName, OutputStream os)
			throws RepresentationNotExistsException, FileNotExistsException;


	void deleteContent(String globalId, String schema, String version, String fileName)
			throws RepresentationNotExistsException, FileNotExistsException, CannotModifyPersistentRepresentationException;


	ResultSlice<Representation> search(String providerId, String representationName, String dataSetId, String thresholdParam, int limit);
}

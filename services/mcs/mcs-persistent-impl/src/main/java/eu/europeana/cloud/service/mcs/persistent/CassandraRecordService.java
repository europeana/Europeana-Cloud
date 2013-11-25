package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author sielski
 */
@Service
public class CassandraRecordService implements RecordService {

	@Autowired
	private CassandraRecordDAO recordDAO;

	@Autowired
	private ContentDAO contentDAO;

	@Autowired
	private CassandraDataSetDAO dataSetDAO;

	@Autowired
	private CassandraDataProviderDAO dataProviderDAO;


	@Override
	public Record getRecord(String globalId)
			throws RecordNotExistsException {
		return recordDAO.getRecord(globalId);
	}


	@Override
	public void deleteRecord(String globalId)
			throws RecordNotExistsException {
		Record r = recordDAO.getRecord(globalId);
		for (Representation rep : r.getRepresentations()) {
			for (Representation repVersion : recordDAO.listRepresentationVersions(globalId, rep.getSchema())) {
				for (File f : repVersion.getFiles()) {
					contentDAO.deleteContent(generateKeyForFile(globalId, repVersion.getSchema(), repVersion.
							getVersion(), f.getFileName()));
				}
			}
		}
		recordDAO.deleteRecord(globalId);
	}


	@Override
	public void deleteRepresentation(String globalId, String schema)
			throws RecordNotExistsException, RepresentationNotExistsException {
		for (Representation rep : recordDAO.listRepresentationVersions(globalId, schema)) {
			for (File f : rep.getFiles()) {
				contentDAO.deleteContent(generateKeyForFile(globalId, schema, rep.getVersion(), f.getFileName()));
			}
		}
		recordDAO.deleteRepresentation(globalId, schema);
	}


	@Override
	public Representation createRepresentation(String globalId, String representationName, String providerId)
			throws ProviderNotExistsException {
		// check if data provider exists
		if (dataProviderDAO.getProvider(providerId) == null) {
			throw new ProviderNotExistsException();
		}
		return recordDAO.createRepresentation(globalId, representationName, providerId);
	}


	@Override
	public Representation getRepresentation(String globalId, String schema)
			throws RepresentationNotExistsException {
		Representation rep = recordDAO.getLatestPersistentRepresentation(globalId, schema);
		if (rep == null) {
			throw new RepresentationNotExistsException();
		} else {
			return rep;
		}
	}


	@Override
	public Representation getRepresentation(String globalId, String schema, String version)
			throws RepresentationNotExistsException {
		Representation rep = recordDAO.getRepresentation(globalId, schema, version);
		if (rep == null) {
			throw new RepresentationNotExistsException();
		} else {
			return rep;
		}
	}


	@Override
	public void deleteRepresentation(String globalId, String schema, String version)
			throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
		Representation rep = recordDAO.getRepresentation(globalId, schema, version);
		if (rep == null) {
			throw new RepresentationNotExistsException();
		}
		if (rep.isPersistent()) {
			throw new CannotModifyPersistentRepresentationException();
		}
		for (File f : rep.getFiles()) {
			contentDAO.deleteContent(generateKeyForFile(globalId, schema, version, f.getFileName()));
		}
		recordDAO.deleteRepresentation(globalId, schema, version);
	}


	@Override
	public Representation persistRepresentation(String globalId, String schema, String version)
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException {
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
		recordDAO.persistRepresentation(globalId, schema, version);
		rep.setPersistent(true);
		return rep;
	}


	@Override
	public List<Representation> listRepresentationVersions(String globalId, String schema)
			throws RecordNotExistsException, RepresentationNotExistsException {
		return recordDAO.listRepresentationVersions(globalId, schema);
	}


	@Override
	public boolean putContent(String globalId, String schema, String version, File file, InputStream content)
			throws FileAlreadyExistsException, IOException {
		DateTime now = new DateTime();
		Representation representation = recordDAO.getRepresentation(globalId, schema, version);
		if (representation.isPersistent()) {
			throw new CannotModifyPersistentRepresentationException();
		}

		boolean isCreate = true; // if it is create file operation or update content
		if (file.getFileName() == null) {
			file.setFileName(UUID.randomUUID().toString());
		} else {
			for (File f : representation.getFiles()) {
				if (f.getFileName().equals(file.getFileName())) {
					isCreate = false;
					break;
				}
			}
		}

		String keyForFile = generateKeyForFile(globalId, schema, version, file.getFileName());
		PutResult result = contentDAO.putContent(keyForFile, content);
		file.setMd5(result.getMd5());
		DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
		file.setDate(fmt.print(now));
		file.setContentLength(result.getContentLength());
		recordDAO.addOrReplaceFileInRepresentation(globalId, schema, version, file);
		return isCreate;
	}


	@Override
	public void getContent(String globalId, String schema, String version, String fileName, long rangeStart, long rangeEnd, OutputStream os)
			throws FileNotExistsException, IOException {
		contentDAO.getContent(generateKeyForFile(globalId, schema, version, fileName), rangeStart, rangeEnd, os);
	}


	@Override
	public String getContent(String globalId, String schema, String version, String fileName, OutputStream os)
			throws FileNotExistsException, IOException {
		Representation rep = getRepresentation(globalId, schema, version);
		String md5 = null;
		for (File f : rep.getFiles()) {
			if (fileName.equals(f.getFileName())) {
				md5 = f.getMd5();
			}
		}
		if (md5 == null) {
			throw new FileNotExistsException();
		}
		contentDAO.getContent(generateKeyForFile(globalId, schema, version, fileName), -1, -1, os);
		return md5;
	}


	@Override
	public void deleteContent(String globalId, String schema, String version, String fileName)
			throws FileNotExistsException {
		Representation representation = recordDAO.getRepresentation(globalId, schema, version);
		if (representation.isPersistent()) {
			throw new CannotModifyPersistentRepresentationException();
		}
		recordDAO.removeFileFromRepresentation(globalId, schema, version, fileName);
		contentDAO.deleteContent(generateKeyForFile(globalId, schema, version, fileName));
	}


	@Override
	public Representation copyRepresentation(String globalId, String schema, String version)
			throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException {
		Representation srcRep = recordDAO.getRepresentation(globalId, schema, version);
		Representation copiedRep = recordDAO.createRepresentation(globalId, schema, srcRep.getDataProvider());
		for (File srcFile : srcRep.getFiles()) {
			File copiedFile = new File(srcFile);
			copyRepresentation(globalId, schema, version);
			contentDAO.copyContent(generateKeyForFile(globalId, schema, version, srcFile.getFileName()),
					generateKeyForFile(globalId, schema, copiedRep.getVersion(), copiedFile.getFileName()));
			recordDAO.addOrReplaceFileInRepresentation(globalId, schema, copiedRep.getVersion(), copiedFile);
		}
		//get version after all modifications
		return recordDAO.getRepresentation(globalId, schema, copiedRep.getVersion());
	}


	@Override
	public ResultSlice<Representation> search(String providerId, String schema, String dataSetId, String thresholdParam, int limit) {
		throw new UnsupportedOperationException("Not implemented");
	}


	private String generateKeyForFile(String recordId, String repName, String version, String fileName) {
		return recordId + "|" + repName + "|" + version + "|" + fileName;
	}

}

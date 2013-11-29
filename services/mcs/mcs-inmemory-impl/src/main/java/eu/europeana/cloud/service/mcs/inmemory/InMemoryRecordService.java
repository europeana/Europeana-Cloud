package eu.europeana.cloud.service.mcs.inmemory;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * InMemoryContentServiceImpl
 */
@Service
public class InMemoryRecordService implements RecordService {

	@Autowired
	private InMemoryRecordDAO recordDAO;

	@Autowired
	private InMemoryContentDAO contentDAO;

	@Autowired
	private InMemoryDataSetDAO dataSetDAO;

	@Autowired
	private InMemoryDataProviderDAO dataProviderDAO;


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
			try {
				for (Representation repVersion : recordDAO.listRepresentationVersions(globalId, rep.getSchema())) {
					for (File f : repVersion.getFiles()) {
						contentDAO.deleteContent(globalId, repVersion.getSchema(), repVersion.getVersion(), f.
								getFileName());
					}
				}
			} catch (RepresentationNotExistsException | FileNotExistsException ex) {
				// should not happen
			}
		}
		recordDAO.deleteRecord(globalId);
	}


	@Override
	public void deleteRepresentation(String globalId, String schema)
			throws RepresentationNotExistsException {
		for (Representation rep : recordDAO.listRepresentationVersions(globalId, schema)) {
			for (File f : rep.getFiles()) {
				try {
					contentDAO.deleteContent(globalId, schema, rep.getVersion(), f.getFileName());
				} catch (FileNotExistsException ex) {
					// should not happen
				}
			}
		}
		recordDAO.deleteRepresentation(globalId, schema);
	}


	@Override
	public Representation createRepresentation(String globalId, String representationName, String providerId)
			throws RecordNotExistsException, ProviderNotExistsException {
		dataProviderDAO.getProvider(providerId);
		return recordDAO.createRepresentation(globalId, representationName, providerId);
	}


	@Override
	public Representation getRepresentation(String globalId, String schema)
			throws RepresentationNotExistsException {
		return recordDAO.getRepresentation(globalId, schema, null);
	}


	@Override
	public Representation getRepresentation(String globalId, String schema, String version)
			throws RepresentationNotExistsException {
		return recordDAO.getRepresentation(globalId, schema, version);
	}


	@Override
	public void deleteRepresentation(String globalId, String schema, String version)
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
		Representation rep = recordDAO.getRepresentation(globalId, schema, version);
		for (File f : rep.getFiles()) {
			try {
				contentDAO.deleteContent(globalId, schema, version, f.getFileName());
			} catch (FileNotExistsException ex) {
				// should not happen
			}
		}
		recordDAO.deleteRepresentation(globalId, schema, version);
	}


	@Override
	public Representation persistRepresentation(String globalId, String schema, String version)
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
		return recordDAO.persistRepresentation(globalId, schema, version);
	}


	@Override
	public List<Representation> listRepresentationVersions(String globalId, String schema)
			throws RepresentationNotExistsException {
		return recordDAO.listRepresentationVersions(globalId, schema);
	}


	@Override
	public boolean putContent(String globalId, String schema, String version, File file, InputStream content)
			throws FileAlreadyExistsException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
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
		try {
			contentDAO.putContent(globalId, schema, version, file, content);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
		recordDAO.addOrReplaceFileInRepresentation(globalId, schema, version, file);
		return isCreate;
	}


	@Override
	public void getContent(String globalId, String schema, String version, String fileName, long rangeStart, long rangeEnd, OutputStream os)
			throws FileNotExistsException, WrongContentRangeException {
		try {
			contentDAO.getContent(globalId, schema, version, fileName, rangeStart, rangeEnd, os);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}


	@Override
	public String getContent(String globalId, String schema, String version, String fileName, OutputStream os)
			throws FileNotExistsException, RepresentationNotExistsException {
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
		try {
			contentDAO.getContent(globalId, schema, version, fileName, -1, -1, os);
		} catch (IOException | WrongContentRangeException ex) {
			throw new RuntimeException(ex);
		}
		return md5;
	}


	@Override
	public void deleteContent(String globalId, String schema, String version, String fileName)
			throws FileNotExistsException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
		recordDAO.removeFileFromRepresentation(globalId, schema, version, fileName);
		contentDAO.deleteContent(globalId, schema, version, fileName);
	}


	@Override
	public Representation copyRepresentation(String globalId, String schema, String version)
			throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
		Representation srcRep = recordDAO.getRepresentation(globalId, schema, version);
		Representation copiedRep = recordDAO.createRepresentation(globalId, schema, srcRep.getDataProvider());
		for (File srcFile : srcRep.getFiles()) {
			File copiedFile = new File(srcFile);
			copyRepresentation(globalId, schema, version);
			try {
				contentDAO.copyContent(globalId, schema, version, srcFile.getFileName(),
						globalId, schema, copiedRep.getVersion(), copiedFile.getFileName());
				recordDAO.addOrReplaceFileInRepresentation(globalId, schema, copiedRep.getVersion(), copiedFile);
			} catch (FileNotExistsException | FileAlreadyExistsException | CannotModifyPersistentRepresentationException ex) {
				// should not happen
			}
		}
		//get version after all modifications
		return recordDAO.getRepresentation(globalId, schema, copiedRep.getVersion());
	}


	@Override
	public ResultSlice<Representation> search(String providerId, String schema, String dataSetId, String thresholdParam, int limit) {
		if (thresholdParam != null) {
			throw new UnsupportedOperationException("Paging with threshold is not supported");
		}
		List<Representation> result;
		if (providerId != null && dataSetId != null) {
			// get all for dataset then filter for schema
			List<Representation> toReturn = new ArrayList<>();
			try {
				List<Representation> representationStubs = dataSetDAO.listDataSet(providerId, dataSetId);
				for (Representation stub : representationStubs) {
					if (schema == null || schema.equals(stub.getSchema())) {
						Representation realContent;
						try {
							realContent = recordDAO.
									getRepresentation(stub.getRecordId(), stub.getSchema(), stub.getVersion());
							toReturn.add(realContent);
						} catch (RepresentationNotExistsException ex) {
							// ignore
						}

					}
				}
			} catch (DataSetNotExistsException ex) {
				result = Collections.EMPTY_LIST;
			}
			result = toReturn;
		} else {
			result = recordDAO.findRepresentations(providerId, schema);
		}
		result = result.subList(0, Math.min(limit, result.size()));
		return new ResultSlice<>(null, result);
	}


	@Override
	public File getFile(String globalId, String schema, String version, String fileName, OutputStream os)
			throws RepresentationNotExistsException, FileNotExistsException {
		final Representation rep = getRepresentation(globalId, schema, version);
		for (File f : rep.getFiles()) {
			if (f.getFileName().equals(fileName)) {
				return f;
			}
		}

		throw new FileNotExistsException();
	}
}

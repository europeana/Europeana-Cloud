package eu.europeana.cloud.service.mcs.service;

import java.util.List;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;

/**
 * ContentService
 */
public interface RecordService {

    Record getRecord(String globalId)
            throws RecordNotExistsException;


    void deleteRecord(String globalId)
            throws RecordNotExistsException;

//==

    Representation getRepresentation(String globalId, String representationName)
            throws RecordNotExistsException, RepresentationNotExistsException;


    void deleteRepresentation(String globalId, String representationName)
            throws RecordNotExistsException, RepresentationNotExistsException;


    Representation createRepresentation(String globalId, String representationName, String providerId)
            throws ProviderNotExistsException;


    List<Representation> listRepresentationVersions(String globalId, String representationName)
            throws RecordNotExistsException, RepresentationNotExistsException;

    //==

    Representation getRepresentation(String globalId, String representationName, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException;


    void deleteRepresentation(String globalId, String representationName, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException;


    Representation persistRepresentation(String globalId, String representationName, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException;


    Representation addFileToRepresentation(String globalId, String representationName, String version, File f)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, FileAlreadyExistsException;
}

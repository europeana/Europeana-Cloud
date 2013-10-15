package eu.europeana.cloud.contentserviceapi.service;

import java.util.List;

import eu.europeana.cloud.contentserviceapi.exception.CannotDeletePersistentRepresentationVersion;
import eu.europeana.cloud.contentserviceapi.exception.ProviderNotExistsException;
import eu.europeana.cloud.contentserviceapi.exception.RecordNotExistsException;
import eu.europeana.cloud.contentserviceapi.exception.RepresentationAlreadyPersistentException;
import eu.europeana.cloud.contentserviceapi.exception.RepresentationNotExistsException;
import eu.europeana.cloud.contentserviceapi.exception.VersionNotExistsException;
import eu.europeana.cloud.definitions.model.File;
import eu.europeana.cloud.definitions.model.Record;
import eu.europeana.cloud.definitions.model.Representation;

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
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, CannotDeletePersistentRepresentationVersion;


    Representation persistRepresentation(String globalId, String representationName, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, RepresentationAlreadyPersistentException;


}

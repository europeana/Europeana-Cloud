package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import eu.europeana.cloud.common.response.ErrorInfo;
import javax.ws.rs.core.Response;

import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileContentHashMismatchException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyInSetException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;

/**
 * AllExceptionMapper
 */
public class UnitedExceptionMapper {

    final static int UNPROCESSABLE_ENTITY = 422;


    public Response toResponse(CannotModifyPersistentRepresentationException exception) {
        return buildResponse(Response.Status.METHOD_NOT_ALLOWED, McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION, exception);
    }


    public Response toResponse(DataSetAlreadyExistsException exception) {
        return buildResponse(Response.Status.CONFLICT, McsErrorCode.DATASET_ALREADY_EXISTS, exception);
    }


    public Response toResponse(DataSetNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.DATASET_NOT_EXISTS, exception);
    }


    public Response toResponse(FileAlreadyExistsException exception) {
        return buildResponse(Response.Status.CONFLICT, McsErrorCode.FILE_ALREADY_EXISTS, exception);
    }


    public Response toResponse(FileNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.FILE_NOT_EXISTS, exception);
    }


    public Response toResponse(ProviderHasDataSetsException exception) {
        return buildResponse(Response.Status.METHOD_NOT_ALLOWED, McsErrorCode.PROVIDER_HAS_DATASETS, exception);
    }


    public Response toResponse(ProviderHasRecordsException exception) {
        return buildResponse(Response.Status.METHOD_NOT_ALLOWED, McsErrorCode.PROVIDER_HAS_RECORDS, exception);
    }


    public Response toResponse(ProviderNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.PROVIDER_NOT_EXISTS, exception);
    }


    public Response toResponse(RecordNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.RECORD_NOT_EXISTS, exception);
    }


    public Response toResponse(RepresentationNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.REPRESENTATION_NOT_EXISTS, exception);
    }


    public Response toResponse(VersionNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.VERSION_NOT_EXISTS, exception);
    }


    public Response toResponse(FileContentHashMismatchException exception) {
        return buildResponse(UNPROCESSABLE_ENTITY, McsErrorCode.FILE_CONTENT_HASH_MISMATCH, exception);
    }


    public Response toResponse(RepresentationAlreadyInSetException exception) {
        return buildResponse(Response.Status.CONFLICT, McsErrorCode.REPRESENTATION_ALREADY_IN_SET, exception);
    }


    private static Response buildResponse(Response.Status httpStatus, McsErrorCode errorCode, Exception e) {
        return buildResponse(httpStatus.getStatusCode(), errorCode, e);
    }


    private static Response buildResponse(int httpStatusCode, McsErrorCode errorCode, Exception e) {
        return Response.status(httpStatusCode).entity(new ErrorInfo(errorCode.name(), e.getMessage())).build();
    }
}

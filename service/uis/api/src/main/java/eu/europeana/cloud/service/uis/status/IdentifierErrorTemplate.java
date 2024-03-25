package eu.europeana.cloud.service.uis.status;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.uis.exception.CloudIdAlreadyExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import jakarta.ws.rs.core.Response;

/**
 * Status Messages returned by all methods
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 22, 2013
 */
@SuppressWarnings("unchecked")
public enum IdentifierErrorTemplate {

  /**
   * Unspecified Error - HTTP Code: 500
   */
  GENERIC_ERROR {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo("GENERIC_ERROR", String.format(
          "Unspecified Error occured with message %s", args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.INTERNAL_SERVER_ERROR;
    }

    @Override
    public GenericException getException(ErrorInfo e) {
      return new GenericException(e);

    }

  },
  /**
   * Database Connection Error - HTTP Code: 500
   */
  DATABASE_CONNECTION_ERROR {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo("DATABASE_CONNECTION_ERROR", String.format(
          "The connection to the DB %s/%s failed with error %s",
          args[0], args[1], args[2]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.INTERNAL_SERVER_ERROR;
    }

    @Override
    public DatabaseConnectionException getException(ErrorInfo e) {
      return new DatabaseConnectionException(e);
    }
  },
  /**
   * Record exists already in the database - HTTP code: 409
   */
  RECORD_EXISTS {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo(
          "RECORD_EXISTS",
          String.format(
              "An identifier for provider id %s and record id %s already exists in the database",
              args[0], args[1]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.CONFLICT;
    }

    @Override
    public RecordExistsException getException(ErrorInfo e) {
      return new RecordExistsException(e);
    }
  },
  /**
   * The record does not exist in the database - HTTP code: 404
   */
  RECORD_DOES_NOT_EXIST {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo(
          "RECORD_DOES_NOT_EXIST",
          String.format(
              "A global identifier for provider id %s and record id %s does not exist",
              args[0], args[1]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.NOT_FOUND;
    }

    @Override
    public RecordDoesNotExistException getException(ErrorInfo e) {
      return new RecordDoesNotExistException(e);
    }
  },
  /**
   * The supplied unique identifier already exist - HTTP code: 409
   */
  CLOUDID_ALREADY_EXIST {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo("CLOUDID_ALREADY_EXIST", String.format(
          "The supplied cloud identifier %s already exist", args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.CONFLICT;
    }

    @Override
    public CloudIdAlreadyExistException getException(ErrorInfo e) {
      return new CloudIdAlreadyExistException(e);
    }
  },
  /**
   * The supplied unique identifier does not exist - HTTP code: 404
   */
  CLOUDID_DOES_NOT_EXIST {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo("CLOUDID_DOES_NOT_EXIST", String.format(
          "The supplied cloud identifier %s does not exist", args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.NOT_FOUND;
    }

    @Override
    public CloudIdDoesNotExistException getException(ErrorInfo e) {
      return new CloudIdDoesNotExistException(e);
    }
  },
  /**
   * The provider id does not exist - HTTP code: 404
   */
  PROVIDER_DOES_NOT_EXIST {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo("PROVIDER_DOES_NOT_EXIST", String.format(
          "The supplied provider identifier %s does not exist",
          args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.NOT_FOUND;
    }

    @Override
    public ProviderDoesNotExistException getException(ErrorInfo e) {
      return new ProviderDoesNotExistException(e);
    }
  },
  /**
   * The provider already exists exception
   */
  PROVIDER_ALREADY_EXISTS {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo("PROVIDER_ALREADY_EXISTS", String.format(
          "The provider with identifier %s already exists", args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.CONFLICT;
    }

    @Override
    public ProviderAlreadyExistsException getException(ErrorInfo e) {
      return new ProviderAlreadyExistsException(e);
    }

  },
  /**
   * The record id does not exist - HTTP code: 404
   */
  RECORDID_DOES_NOT_EXIST {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo("RECORDID_DOES_NOT_EXIST",
          String.format(
              "The supplied record identifier %s does not exist",
              args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.NOT_FOUND;
    }

    @Override
    public RecordIdDoesNotExistException getException(ErrorInfo e) {
      return new RecordIdDoesNotExistException(e);
    }
  },
  /**
   * The requested record set for the provider id is empty - HTTP code: 404
   */
  RECORDSET_EMPTY {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo(
          "RECORDSET_EMPTY",
          String.format(
              "The supplied provider %s does not have any records associated with it",
              args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.NOT_FOUND;
    }

    @Override
    public RecordDatasetEmptyException getException(ErrorInfo e) {
      return new RecordDatasetEmptyException(e);
    }

  },
  /**
   * The combination of provider id/ record id has already been mapped to another unique identifier - HTTP code: 409
   */
  ID_HAS_BEEN_MAPPED {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo(
          "ID_HAS_BEEN_MAPPED",
          String.format(
              "The supplied %s id for provider %s has already been assigned to the cloud identifier %s",
              args[1], args[0], args[2]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.CONFLICT;
    }

    @Override
    public IdHasBeenMappedException getException(ErrorInfo e) {
      return new IdHasBeenMappedException(e);
    }
  },

  ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo(
          "ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION",
          "Access is denied");
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.FORBIDDEN;
    }

    @Override
    public GenericException getException(ErrorInfo e) {
      return new GenericException(e);
    }
  },

  OTHER {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo(
          "OTHER",
          String.format(
              "The supplied %s id for provider %s has already been assigned to the cloud identifier %s",
              args[1], args[0], args[2]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.INTERNAL_SERVER_ERROR;
    }

    @Override
    public GenericException getException(ErrorInfo e) {
      return new GenericException(e);
    }
  };


  /**
   * Generate the error message for each case
   *
   * @param args
   * @return The generated error message
   */
  public abstract ErrorInfo getErrorInfo(String... args);

  /**
   * Return the according HTTP Code
   *
   * @return The relevant HTTP Code
   */
  public abstract Response.Status getHttpCode();

  /**
   * Generate an exception according to the type of ErrorCode
   *
   * @param e The related Error information
   * @return A GenericException
   */
  public abstract <T extends GenericException> T getException(ErrorInfo e);

}

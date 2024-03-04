package eu.europeana.cloud.service.aas.authentication.status;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.aas.authentication.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.aas.authentication.exception.InvalidPasswordException;
import eu.europeana.cloud.service.aas.authentication.exception.InvalidUsernameException;
import eu.europeana.cloud.service.aas.authentication.exception.UserDoesNotExistException;
import eu.europeana.cloud.service.aas.authentication.exception.UserExistsException;
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
   * User exists already in the database - HTTP code: 409
   */
  USER_EXISTS {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo(
          "USER_EXISTS",
          String.format(
              "A user %s already exists in the database",
              args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.CONFLICT;
    }

    @Override
    public UserExistsException getException(ErrorInfo e) {
      return new UserExistsException(e);
    }
  },
  /**
   * User does not exist already in the database - HTTP code: 404
   */
  USER_DOES_NOT_EXIST {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo(
          "USER_DOES_NOT_EXIST",
          String.format(
              "A user %s does not exist in the database",
              args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.NOT_FOUND;
    }

    @Override
    public UserDoesNotExistException getException(ErrorInfo e) {
      return new UserDoesNotExistException(e);
    }
  },
  /**
   * The requested record set for the provider id is empty - HTTP code: 404
   */
  INVALID_USERNAME {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo(
          "INVALID_USERNAME",
          String.format(
              "The supplied username %s is not valid!",
              args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.CONFLICT;
    }

    @Override
    public InvalidUsernameException getException(ErrorInfo e) {
      return new InvalidUsernameException(e);
    }

  },
  /**
   * The password is invalid - HTTP code: 404
   */
  INVALID_PASSWORD {
    @Override
    public ErrorInfo getErrorInfo(String... args) {
      return new ErrorInfo(
          "INVALID_PASSWORD",
          String.format(
              "The supplied password %s is not valid!",
              args[0]));
    }

    @Override
    public Response.Status getHttpCode() {
      return Response.Status.CONFLICT;
    }

    @Override
    public InvalidPasswordException getException(ErrorInfo e) {
      return new InvalidPasswordException(e);
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

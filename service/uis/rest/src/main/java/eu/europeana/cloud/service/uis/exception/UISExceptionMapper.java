package eu.europeana.cloud.service.uis.exception;

import javax.ws.rs.core.Response;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;

/**
 * Generic class exposing the exceptions
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class UISExceptionMapper {

	/**
	 * @param e A {@link CloudIdDoesNotExistException}
	 * @return An API exception response corresponding to the exception
	 */
	public Response toResponse(CloudIdDoesNotExistException e){
		return buildResponse(e);
	}
	/**
	 * @param e A {@link DatabaseConnectionException}
	 * @return An API exception response corresponding to the exception
	 */
	public Response toResponse(DatabaseConnectionException e){
		return buildResponse(e);
	}
	/**
	 * @param e An {@link IdHasBeenMappedException}
	 * @return An API exception response corresponding to the exception
	 */
	public Response toResponse(IdHasBeenMappedException e){
		return buildResponse(e);
	}
	/**
	 * @param e A {@link ProviderDoesNotExistException}
	 * @return An API exception response corresponding to the exception
	 */
	public Response toResponse(ProviderDoesNotExistException e){
		return buildResponse(e);
	}
	/**
	 * @param e A {@link RecordDatasetEmptyException}
	 * @return An API exception response corresponding to the exception
	 */
	public Response toResponse(RecordDatasetEmptyException e){
		return buildResponse(e);
	}
	/**
	 * @param e A {@link RecordDoesNotExistException}
	 * @return An API exception response corresponding to the exception
	 */
	public Response toResponse(RecordDoesNotExistException e){
		return buildResponse(e);
	}
	/**
	 * @param e A {@link RecordExistsException}
	 * @return An API exception response corresponding to the exception
	 */
	public Response toResponse(RecordExistsException e){
		return buildResponse(e);
	}
	/**
	 * @param e A {@link RecordIdDoesNotExistException}
	 * @return An API exception response corresponding to the exception
	 */
	public Response toResponse(RecordIdDoesNotExistException e){
		return buildResponse(e);
	}
	
	/**
	 * @param e A {@link ProviderAlreadyExistsException}
	 * @return An API exception response corresponding to the exception
	 */
	public Response toResponse(ProviderAlreadyExistsException e){
		return buildResponse(e);
	}
	private Response buildResponse(GenericException e){
		return Response.status(e.getErrorInfo().getHttpCode()).entity(e.getErrorInfo().getErrorInfo()).build();
	}
}

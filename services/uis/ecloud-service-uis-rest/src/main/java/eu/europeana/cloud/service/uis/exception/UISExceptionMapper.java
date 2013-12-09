package eu.europeana.cloud.service.uis.exception;

import javax.ws.rs.core.Response;

public class UISExceptionMapper {

	public Response toResponse(CloudIdDoesNotExistException e){
		return buildResponse(e);
	}
	public Response toResponse(DatabaseConnectionException e){
		return buildResponse(e);
	}
	public Response toResponse(IdHasBeenMappedException e){
		return buildResponse(e);
	}
	public Response toResponse(ProviderDoesNotExistException e){
		return buildResponse(e);
	}
	public Response toResponse(RecordDatasetEmptyException e){
		return buildResponse(e);
	}
	public Response toResponse(RecordDoesNotExistException e){
		return buildResponse(e);
	}
	public Response toResponse(RecordExistsException e){
		return buildResponse(e);
	}
	public Response toResponse(RecordIdDoesNotExistException e){
		return buildResponse(e);
	}
	
	private static Response buildResponse(GenericException e){
		return Response.status(e.getErrorInfo().getHttpCode()).entity(e.getErrorInfo().getErrorInfo()).build();
	}
}

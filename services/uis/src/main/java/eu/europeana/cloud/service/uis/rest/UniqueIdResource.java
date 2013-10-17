package eu.europeana.cloud.service.uis.rest;

import javax.ws.rs.core.Response;

/**
 * UniqueId REST API implementation
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public interface UniqueIdResource {

	/**
	 * Invoke the unique identifier creation REST call HTTP call: GET
	 * 
	 * @param providerId
	 * @param recordId
	 * @return JSON/XML response with the unique Identifier or Error Message
	 */
	public Response createRecordId(String providerId, String recordId);

	/**
	 * Invoke the unique Identifier search according to providerId/recordId
	 * combo REST call HTTP call: GET
	 * 
	 * @param providerId
	 * @param recordId
	 * @return JSON/XML response with the unique Identifier or Error Message
	 */
	public Response getGlobalId(String providerId, String recordId);

	/**
	 * Invoke the retrieval of providerId/recordId REST call HTTP call: GET
	 * 
	 * @param globalId
	 * @return JSON/XML response with the list of local ids organized by
	 *         provider or Error Message
	 */
	public Response getLocalIds(String globalId);

	/**
	 * Invoke the retrieval of recordId based on a providerId REST call HTTP
	 * call: GET
	 * 
	 * @param globalId
	 * @param start
	 * @param to
	 * @return JSON/XML response with the list of local ids of that provider or
	 *         Error Message
	 */
	public Response getLocalIdsByProvider(String providerId, int start, int to);

	/**
	 * Invoke the retrieval of all unique identifiers for a giver provider HTTP
	 * call: GET
	 * 
	 * @param providerId
	 * @param start
	 * @param to
	 * @return JSON/XML response with the list of unique ids of that provider or
	 *         Error Message
	 */
	public Response getGlobalIdsByProvider(String providerId, int start, int to);

	/**
	 * Create the mapping between an existing unique identifier and a
	 * providerId/recordId combo HTTP call: GET
	 * 
	 * @param globalId
	 * @param providerId
	 * @param recordId
	 * @return JSON/XML acknowledgement or Error Message
	 */
	public Response createMapping(String globalId, String providerId,
			String recordId);

	/**
	 * Remove the mapping between a providerId/recordId and a unique identifier
	 * HTTP call: DELETE
	 * 
	 * @param providerId
	 * @param recordId
	 * @return JSON/XML acknowledgement or Error Message
	 */
	public Response removeMappingByLocalId(String providerId, String recordId);

	/**
	 * Remove the unique identifier and all of its mappings HTTP call: DELETE
	 * 
	 * @param globalId
	 * @return JSON/XML acknowledgement or Error Message
	 */
	public Response deleteGlobalId(String globalId);

}

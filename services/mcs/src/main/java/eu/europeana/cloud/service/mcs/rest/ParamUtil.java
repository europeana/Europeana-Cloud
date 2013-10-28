package eu.europeana.cloud.service.mcs.rest;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_GID;

/**
 * ParamUtil
 */
final class ParamUtil {

    /**
     * Checks if parameter value is not null. If it is, WebApplicationException is thrown with 400 HTTP code and suitable
     * message in response body indicating name of form parameter name that is required.
     * 
     * @param parameterName form parameter name 
     * @param parameterValue form parameter value
     */
    static void require(String parameterName, Object parameterValue) {
        if (parameterValue == null) {
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                    .entity(parameterName + " is a required parameter").build());
        }
    }


    private ParamUtil() {
    }
}

package eu.europeana.cloud.service.mcs.rest;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import eu.europeana.cloud.common.response.ErrorInfo;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_GID;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;

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
            ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.name(), parameterName + " is a required parameter");
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo).build());
        }
    }


    private ParamUtil() {
    }
}

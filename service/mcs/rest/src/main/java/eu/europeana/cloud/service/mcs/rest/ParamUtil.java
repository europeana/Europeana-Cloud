package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * Util for parameters.
 */
final class ParamUtil {

    /**
     * Checks if parameter value is not null. If it is, WebApplicationException is thrown with 400 HTTP code and
     * suitable message in response body indicating name of form parameter name that is required.
     * 
     * @param parameterName
     *            form parameter name
     * @param parameterValue
     *            form parameter value
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

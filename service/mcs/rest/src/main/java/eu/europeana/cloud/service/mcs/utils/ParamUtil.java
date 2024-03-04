package eu.europeana.cloud.service.mcs.utils;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Set;

/**
 * Util for parameters.
 */
public final class ParamUtil {

  private ParamUtil() {
  }

  /**
   * Checks if parameter value is not null. If it is, WebApplicationException is thrown with 400 HTTP code and suitable message in
   * response body indicating name of form parameter name that is required.
   *
   * @param parameterName form parameter name
   * @param parameterValue form parameter value
   */
  @Deprecated
  public static void require(String parameterName, Object parameterValue) {
    if (parameterValue == null) {
      ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.name(), parameterName + " is a required parameter");
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo).build());
    }
  }

  /**
   * Checks if parameter value is present in acceptedValues list. If it isn't on the list, WebApplicationException is thrown with
   * 400 HTTP code and suitable message in response body indicating name of form parameter name that value is unsupported.
   *
   * @param parameterName form parameter name
   * @param parameterValue form parameter value
   * @param acceptedValues white list of accepted values
   */
  public static <T> void validate(String parameterName, T parameterValue, List<T> acceptedValues) {
    if (!acceptedValues.contains(parameterValue)) {
      ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.name(), parameterName + " has unsupported value");
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo).build());
    }
  }


  /**
   * Checks if the tag set is valid against a set of expected allowed tags
   *
   * @param tags set of tags
   * @param expectedTags set of expected tags
   */
  public static void validateTags(Set<String> tags, Set<String> expectedTags) {
    if (!expectedTags.containsAll(tags)) {
      ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.name(),
          "The tags set was not validated !, The accepted tags are : " + expectedTags);
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo).build());
    }
  }
}

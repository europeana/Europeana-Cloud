package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;

/**
 * Verifies if provided {@link CreateDpsTaskRequest} has MCS output properly defined
 *
 */
public class ProperlyDefinedMCSOutputValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "Output has to be properly defined with providerId, representationName";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(CreateDpsTaskRequest dpsTask) {
    return dpsTask.getResultsProvider() != null;
  }
}

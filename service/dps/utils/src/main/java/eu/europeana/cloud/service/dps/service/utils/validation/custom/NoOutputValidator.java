package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;

/**
 * Verifies if provided {@link CreateDpsTaskRequest} has empty output.
 */
public class NoOutputValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "Output should be empty";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(CreateDpsTaskRequest dpsTask) {
    return dpsTask.getResultsProvider() == null;
  }
}

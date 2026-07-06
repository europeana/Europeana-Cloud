package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.DpsTask;

/**
 * Verifies if provided {@link DpsTask} has empty output.
 */
public class NoOutputValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "Output should be empty";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(DpsTask dpsTask) {
    return dpsTask.getOutputProvider() == null;
  }
}

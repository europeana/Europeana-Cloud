package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsTask;

/**
 * Verifies if provided {@link DpsTask} has MCS output properly defined
 *
 */
public class ProperlyDefinedMCSOutputValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "Output has to be properly defined with providerId, representationName";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(DpsTask dpsTask) {
    if (dpsTask.getOutput() instanceof BatchInfo output) {
      return output.getProviderId() != null &&
          output.getRepresentationName() != null;
    } else {
      return false;
    }
  }
}

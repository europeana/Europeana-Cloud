package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;

/**
 * Verifies if provided {@link CreateDpsTaskRequest} has input that is fully defined. Fully defined BatchInput means that all its
 * properties are provided.
 */
public class FullyDefinedMCSInputValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "Input has to be fully defined - with providerId, representationName and batchId filled";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(CreateDpsTaskRequest dpsTask) {
    if (dpsTask.getSource() instanceof BatchInfo input) {
      return input.getProviderId() != null &&
          input.getBatchId() != null;
    } else {
      return false;
    }
  }
}

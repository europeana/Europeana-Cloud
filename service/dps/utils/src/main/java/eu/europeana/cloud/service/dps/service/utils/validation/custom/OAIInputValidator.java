package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import org.apache.commons.validator.routines.UrlValidator;

/**
 * Verifies if provided {@link DpsTask} has defined OAI input
 */
public class OAIInputValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "OAIPMHHarvestingDetails input should have valid repository url defined.";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(DpsTask dpsTask) {
    if (dpsTask.getInput() instanceof OAIPMHHarvestingDetails input) {
      return input.getRepositoryUrl() != null &&
          new UrlValidator(UrlValidator.ALLOW_LOCAL_URLS).isValid(input.getRepositoryUrl());
    } else {
      return false;
    }
  }
}

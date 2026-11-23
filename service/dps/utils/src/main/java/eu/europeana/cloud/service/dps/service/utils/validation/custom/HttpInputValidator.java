package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.HttpHarvestingDetails;
import org.apache.commons.validator.routines.UrlValidator;

/**
 * Verifies if provided {@link CreateDpsTaskRequest} has defined http input
 */
public class HttpInputValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "HttpHarvestingDetails input should have valid repository url defined.";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(CreateDpsTaskRequest dpsTask) {
    if (dpsTask.getSource() instanceof HttpHarvestingDetails input) {
      return input.getRepositoryUrl() != null &&
          new UrlValidator(UrlValidator.ALLOW_LOCAL_URLS).isValid(input.getRepositoryUrl());
    } else {
      return false;
    }
  }
}

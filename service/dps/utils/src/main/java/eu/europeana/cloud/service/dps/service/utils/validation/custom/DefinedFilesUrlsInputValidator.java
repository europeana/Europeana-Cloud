package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.FilesUrls;
import org.apache.commons.validator.routines.UrlValidator;

/**
 * Verifies if provided {@link DpsTask} has input file urls list valid.
 */
public class DefinedFilesUrlsInputValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "FilesUrls list could not be empty, and should contain only valid URLs.";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(DpsTask dpsTask) {
    if (dpsTask.getInput() instanceof FilesUrls input) {
      UrlValidator urlValidator = new UrlValidator(UrlValidator.ALLOW_LOCAL_URLS);
      return !input.getFileUrls().isEmpty() &&
          input.getFileUrls().stream().allMatch(urlValidator::isValid);
    } else {
      return false;
    }
  }
}

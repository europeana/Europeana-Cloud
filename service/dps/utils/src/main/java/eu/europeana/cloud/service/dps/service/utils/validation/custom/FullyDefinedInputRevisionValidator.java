package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;

/**
 * Verifies if provided {@link DpsTask} has input revision that is fully defined. Fully defined revision means that all its
 * properties are provided (provider_id, revisionName and revisionTimestamp)
 */
public class FullyDefinedInputRevisionValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "Output revision has to be fully defined. Name, provider and creationTimestamp has to be provided.";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(DpsTask dpsTask) {
    return
        dpsTask.getParameter(PluginParameterKeys.REVISION_PROVIDER) != null &&
            !dpsTask.getParameter(PluginParameterKeys.REVISION_PROVIDER).matches("\\s*") &&
            dpsTask.getParameter(PluginParameterKeys.REVISION_NAME) != null &&
            !dpsTask.getParameter(PluginParameterKeys.REVISION_NAME).matches("\\s*") &&
            dpsTask.getParameter(PluginParameterKeys.REVISION_TIMESTAMP) != null;
  }
}

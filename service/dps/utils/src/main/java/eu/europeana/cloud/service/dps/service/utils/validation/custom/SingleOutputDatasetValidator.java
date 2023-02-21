package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;

/**
 * Verifies if OUTPUT_DATA_SETS contains only one entry
 */
public class SingleOutputDatasetValidator extends CustomValidator {

  private static final String MESSAGE = "There should be exactly one output dataset.";

  @Override
  public boolean test(DpsTask dpsTask) {
    return dpsTask.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS) != null
        && dpsTask.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS).split(",").length == 1;
  }

  @Override
  public String detailedMessage() {
    return MESSAGE;
  }
}

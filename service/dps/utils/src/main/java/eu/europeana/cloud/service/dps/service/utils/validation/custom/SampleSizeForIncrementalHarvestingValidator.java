package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;

/**
 * Verifies if {@link PluginParameterKeys.SAMPLE_SIZE} is not set in case of the incremental harvesting
 */
public class SampleSizeForIncrementalHarvestingValidator extends CustomValidator {

    private static final String MESSAGE = "Incremental harvesting could not set " + PluginParameterKeys.SAMPLE_SIZE;

    @Override
    public String detailedMessage() {
        return MESSAGE;
    }

    @Override
    public boolean test(DpsTask dpsTask) {
        if (isIncremental(dpsTask)) {
            return dpsTask.getParameter(PluginParameterKeys.SAMPLE_SIZE) == null;
        } else {
            return true;
        }
    }

    private boolean isIncremental(DpsTask dpsTask) {
        return "true".equals(dpsTask.getParameter(PluginParameterKeys.INCREMENTAL_HARVEST));
    }
}

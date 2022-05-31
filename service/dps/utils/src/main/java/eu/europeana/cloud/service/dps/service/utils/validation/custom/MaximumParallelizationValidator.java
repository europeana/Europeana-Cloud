package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;

public class MaximumParallelizationValidator extends CustomValidator {

    public String detailedMessage() {
        return PluginParameterKeys.MAXIMUM_PARALLELIZATION + " must be positive int value (between 1 and " + Integer.MAX_VALUE + ")";
    }

    @Override
    public boolean test(DpsTask dpsTask) {
        String valueString = dpsTask.getParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION);
        if (valueString == null) {
            return true;
        }

        try {
            int value = Integer.parseInt(valueString);
            return value >= 1;
        } catch (NumberFormatException e) {
            return false;
        }

    }
}

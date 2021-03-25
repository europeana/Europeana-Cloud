package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;

import java.util.function.Predicate;

import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;


abstract class CustomValidator implements Predicate<DpsTask> {

    public String errorMessage() {
        return "[" + this.getClass().getSimpleName() + "]. " + detailedMessage();
    }
    public abstract String detailedMessage();
}

/**
 * Verifies if REPOSITORY_URLS contains only one entry
 */
class SingleRepositoryValidator extends CustomValidator {

    private static final String MESSAGE = "There is more than one repository in input parameters.";

    @Override
    public boolean test(DpsTask dpsTask) {
        return dpsTask.getDataEntry(REPOSITORY_URLS).size() == 1;
    }

    @Override
    public String detailedMessage() {
        return MESSAGE;
    }
}

/**
 * Verifies if OUTPUT_DATA_SETS contains only one entry
 */
class SingleOutputDatasetValidator extends CustomValidator {

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
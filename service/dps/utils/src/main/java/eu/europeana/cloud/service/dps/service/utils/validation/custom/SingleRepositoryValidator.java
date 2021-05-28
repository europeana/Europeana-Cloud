package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.DpsTask;

import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;

/**
 * Verifies if REPOSITORY_URLS contains only one entry
 */
public class SingleRepositoryValidator extends CustomValidator {

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

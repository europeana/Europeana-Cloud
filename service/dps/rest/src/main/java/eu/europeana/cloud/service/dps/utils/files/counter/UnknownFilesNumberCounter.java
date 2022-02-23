package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;

import static eu.europeana.cloud.common.model.dps.TaskInfo.UNKNOWN_EXPECTED_RECORDS_NUMBER;

public class UnknownFilesNumberCounter extends FilesCounter{

    @Override
    public int getFilesCount(DpsTask task) {
        return UNKNOWN_EXPECTED_RECORDS_NUMBER;
    }
}

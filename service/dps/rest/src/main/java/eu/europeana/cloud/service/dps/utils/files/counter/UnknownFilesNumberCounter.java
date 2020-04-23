package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;

public class UnknownFilesNumberCounter extends FilesCounter{

    private static final int UNKNOWN_EXPECTED_SIZE = -1;

    @Override
    public int getFilesCount(DpsTask task) {
        return UNKNOWN_EXPECTED_SIZE;
    }
}

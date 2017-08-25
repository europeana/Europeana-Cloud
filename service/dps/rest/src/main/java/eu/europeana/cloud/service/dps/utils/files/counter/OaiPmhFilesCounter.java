package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;

/**
 * @author akrystian.
 */
public class OaiPmhFilesCounter  extends FilesCounter {

    @Override
    public int getFilesCount(DpsTask task, String authorizationHeader) throws TaskSubmissionException {
        return 1; //TODO add implementation in future issues
    }
}

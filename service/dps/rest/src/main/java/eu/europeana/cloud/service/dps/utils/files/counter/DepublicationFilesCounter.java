package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.depublish.DatasetDepublisher;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import eu.europeana.indexing.exception.IndexingException;

import java.io.IOException;
import java.net.URISyntaxException;

public class DepublicationFilesCounter extends FilesCounter{

    private final DatasetDepublisher depublisher;

    public DepublicationFilesCounter(DatasetDepublisher depublisher){
        this.depublisher=depublisher;
    }

    @Override
    public int getFilesCount(DpsTask task) throws TaskSubmissionException {
        try {
            long expectedSize = depublisher.getRecordsCount(SubmitTaskParameters.builder().task(task).build());

            if (expectedSize > Integer.MAX_VALUE) {
                throw new TaskSubmissionException("There are " + expectedSize + " records in set. It exceeds Integer size and is not supported.");
            }
            if (expectedSize <= 0) {
                throw new TaskSubmissionException("Not found any publicised records of dataset for task " + task.getTaskId());
            }
            return (int)expectedSize;
        } catch (IOException | URISyntaxException | IndexingException e) {
            throw new TaskSubmissionException("Can't evaluate task expected size!",e);
        }
    }
}

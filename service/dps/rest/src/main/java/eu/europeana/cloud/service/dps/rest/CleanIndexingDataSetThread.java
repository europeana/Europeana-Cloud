package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaningException;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.util.concurrent.CompletableFuture;

@Component
@Scope("prototype")
public class CleanIndexingDataSetThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(CleanIndexingDataSetThread.class);

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    private long taskId;

    private DataSetCleanerParameters cleanerParameters;

    private CompletableFuture<ResponseEntity> responseFuture;

    public CleanIndexingDataSetThread(long taskId, DataSetCleanerParameters cleanerParameters,
                                      CompletableFuture<ResponseEntity> responseFuture) {
        super("clean-indexing-dataSet-thread");
        this.taskId = taskId;
        this.cleanerParameters = cleanerParameters;
        this.responseFuture = responseFuture;
    }

    @Override
    public void run() {
        try {
            responseFuture.complete(ResponseEntity.ok("The request was received successfully"));

            if (!areParametersNull(cleanerParameters)) {
                LOGGER.info("cleaning dataset {} based on date: {}",
                        cleanerParameters.getDataSetId(), cleanerParameters.getCleaningDate());
                DatasetCleaner datasetCleaner = new DatasetCleaner(cleanerParameters);
                datasetCleaner.execute();
                LOGGER.info("Dataset {} cleaned successfully", cleanerParameters.getDataSetId());
                taskInfoDAO.setTaskStatus(taskId, "Completely process",
                        TaskState.PROCESSED.toString());
            } else {
                taskInfoDAO.dropTask(taskId, "cleaner parameters can not be null",
                        TaskState.DROPPED.toString());
            }
        } catch (ParseException | DatasetCleaningException e) {
            LOGGER.error("Dataset was not removed correctly. ", e);
            taskInfoDAO.dropTask(taskId, e.getMessage(), TaskState.DROPPED.toString());
        }
    }

    boolean areParametersNull(DataSetCleanerParameters cleanerParameters) {
        return cleanerParameters == null ||
                (cleanerParameters.getDataSetId() == null
                        && cleanerParameters.getTargetIndexingEnv() == null
                        && cleanerParameters.getCleaningDate() == null);
    }
}

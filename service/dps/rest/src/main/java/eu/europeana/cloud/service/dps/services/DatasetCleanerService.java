package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaningException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.text.ParseException;

@Service
public class DatasetCleanerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetCleanerService.class);

    @Autowired
    private TaskStatusUpdater taskStatusUpdater;

    @Async
    public void clean(String taskId, DataSetCleanerParameters cleanerParameters){

        try {
            if (!areParametersNull(cleanerParameters)) {
                LOGGER.info("cleaning dataset {} based on date: {}",
                        cleanerParameters.getDataSetId(), cleanerParameters.getCleaningDate());
                DatasetCleaner datasetCleaner = new DatasetCleaner(cleanerParameters);
                datasetCleaner.execute();
                LOGGER.info("Dataset {} cleaned successfully", cleanerParameters.getDataSetId());
                taskStatusUpdater.setTaskCompletelyProcessed(Long.parseLong(taskId), "Completely process");
            } else {
                taskStatusUpdater.setTaskDropped(Long.parseLong(taskId), "cleaner parameters can not be null");
            }
        } catch (ParseException | DatasetCleaningException e) {
            LOGGER.error("Dataset was not removed correctly. ", e);
            taskStatusUpdater.setTaskDropped(Long.parseLong(taskId), e.getMessage());
        }
    }

    boolean areParametersNull(DataSetCleanerParameters cleanerParameters) {
        return cleanerParameters == null ||
                (cleanerParameters.getDataSetId() == null
                        && cleanerParameters.getTargetIndexingEnv() == null
                        && cleanerParameters.getCleaningDate() == null);
    }

}

package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaningException;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.text.ParseException;

@Service
public class DatasetCleanerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetCleanerService.class);

    private CassandraTaskInfoDAO taskInfoDAO;

    public DatasetCleanerService(CassandraTaskInfoDAO taskInfoDAO){
        this.taskInfoDAO = taskInfoDAO;
    }

    @Async
    public void clean(String taskId, DataSetCleanerParameters cleanerParameters){

        try {
            if (cleanerParameters != null) {
                LOGGER.info("cleaning dataset {} based on date: {}",
                        cleanerParameters.getDataSetId(), cleanerParameters.getCleaningDate());
                DatasetCleaner datasetCleaner = new DatasetCleaner(cleanerParameters);
                datasetCleaner.execute();
                LOGGER.info("Dataset {} cleaned successfully", cleanerParameters.getDataSetId());
                taskInfoDAO.setTaskStatus(Long.parseLong(taskId), "Completely process", TaskState.PROCESSED.toString());
            } else {
                taskInfoDAO.dropTask(Long.parseLong(taskId), "cleaner parameters can not be null",
                        TaskState.DROPPED.toString());
            }
        } catch (ParseException | DatasetCleaningException e) {
            LOGGER.error("Dataset was not removed correctly. ", e);
            taskInfoDAO.dropTask(Long.parseLong(taskId), e.getMessage(), TaskState.DROPPED.toString());
        }
    }
}

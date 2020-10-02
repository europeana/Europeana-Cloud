package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;

@Service
public class CleanTaskDirService {
    private static final Logger LOGGER = LoggerFactory.getLogger(CleanTaskDirService.class);
    private static final String TASK_DIR_PREFIX = "task_";

    @Value("${harvestingTasksDir}")
    private String harvestingTasksDir;

    private CassandraTaskInfoDAO taskInfoDAO;

    private File tasksDir;

    public CleanTaskDirService(CassandraTaskInfoDAO taskInfoDAO) {
        taskInfoDAO = taskInfoDAO;
    }

    @PostConstruct
    private void checkHarvestingTasksDir() {
        tasksDir = new File(harvestingTasksDir);

        if(!tasksDir.exists() || !tasksDir.isDirectory()) {
            LOGGER.error("Given harvesting tasks directory '{}' doesn't exist or it isn't directory", harvestingTasksDir);
        }
    }

    @Scheduled(cron = "0 0 * * * *")
    public void serviceTask() {

    }

    public static final String getDirName(String tasksDirName, long taskId) {
        if (tasksDirName == null)
            throw new NullPointerException("tasksDirName cannot be null");

        

        return tasksDirName + TASK_DIR_PREFIX + Long.toString(taskId);
    }
}

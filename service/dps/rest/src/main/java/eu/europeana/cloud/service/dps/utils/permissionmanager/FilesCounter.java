package eu.europeana.cloud.service.dps.utils.permissionmanager;

import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.List;

/**
 * Created by Tarek on 4/6/2016.
 */
public abstract class FilesCounter {
    protected ApplicationContext context;

    protected FilesCounter(ApplicationContext context) {
        this.context = context;
    }

    /**
     * @return The number of records inside the task.
     */
    public abstract int getFilesCount(DpsTask task, String authorizationHeader) throws TaskSubmissionException;
}

package eu.europeana.cloud.service.dps.utils.permissionmanager;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import org.springframework.context.ApplicationContext;

/**
 * Created by Tarek on 4/6/2016.
 */
public class FilesCounterFactory {
    private ApplicationContext context;

    public FilesCounterFactory(ApplicationContext context) {
        this.context = context;
    }

    public FilesCounter createFilesCounter(String taskType) {
        if (PluginParameterKeys.FILE_URLS.equals(taskType))
            return new RecordFilesCounter(context);
        if (PluginParameterKeys.DATASET_URLS.equals(taskType))
            return new DatasetFilesCounter(context);
        else
            return null;
    }
}

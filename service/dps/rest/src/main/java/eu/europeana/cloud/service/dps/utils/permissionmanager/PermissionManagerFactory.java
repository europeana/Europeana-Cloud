package eu.europeana.cloud.service.dps.utils.permissionmanager;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import org.springframework.context.ApplicationContext;

/**
 * Created by Tarek on 4/6/2016.
 */
public class PermissionManagerFactory {
    private ApplicationContext context;

    public PermissionManagerFactory(ApplicationContext context) {
        this.context = context;
    }

    public ResourcePermissionManager createPermissionManager(String taskType) {
        if (PluginParameterKeys.FILE_URLS.equals(taskType))
            return new RecordPermissionManager(context);
        if (PluginParameterKeys.DATASET_URLS.equals(taskType))
            return new DatasetPermissionManager(context);
        else
            return null;
    }
}

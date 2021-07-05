package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;

import javax.inject.Inject;
import java.util.Map;

public class PostProcessorFactory {
    private final Map<String, TaskPostProcessor> services;

    @Inject
    public PostProcessorFactory(Map<String, TaskPostProcessor> services) {
        this.services = services;
    }

    public TaskPostProcessor getPostProcessor(TaskInfo taskInfo) {
        return services.get(taskInfo.getTopologyName());
    }
}

package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;

import javax.inject.Inject;
import java.util.Map;

public class PostProcessorFactory {
    private final Map<String, TaskPostProcessor> services;

    @Inject
    public PostProcessorFactory(Map<String, TaskPostProcessor> services) {
        this.services = services;
    }

    public TaskPostProcessor getPostProcessor(TaskByTaskState taskByTaskState) {
        return services.get(taskByTaskState.getTopologyName());
    }
}

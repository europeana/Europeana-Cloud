package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;

import java.util.Map;
import java.util.Optional;

public class PostProcessorFactory {
    private final Map<String, TaskPostProcessor> services;

    public PostProcessorFactory(Map<String, TaskPostProcessor> services) {
        this.services = services;
    }

    public TaskPostProcessor getPostProcessor(TaskByTaskState taskByTaskState) {
        if(!services.containsKey(taskByTaskState.getTopologyName())) {
            throw new PostProcessingException(String.format("No PostProcessor for given topology: '%s'", taskByTaskState.getTopologyName()));
        }

        return services.get(taskByTaskState.getTopologyName());
    }
}

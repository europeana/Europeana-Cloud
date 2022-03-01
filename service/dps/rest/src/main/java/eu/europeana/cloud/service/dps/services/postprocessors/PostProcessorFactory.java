package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostProcessorFactory {
    private final Map<String, TaskPostProcessor> services;

    public PostProcessorFactory(List<TaskPostProcessor> services) {
        Map<String, TaskPostProcessor> postProcessorsMap = new HashMap<>();

        services.forEach(postProcessor ->
                postProcessor.getProcessedTopologies().forEach(topologyName -> postProcessorsMap.put(topologyName, postProcessor))
        );

        this.services = Collections.unmodifiableMap(postProcessorsMap);
    }

    public TaskPostProcessor getPostProcessor(TaskByTaskState taskByTaskState) {
        if (!services.containsKey(taskByTaskState.getTopologyName())) {
            throw new PostProcessingException(String.format("No PostProcessor for given topology: '%s'", taskByTaskState.getTopologyName()));
        }

        return services.get(taskByTaskState.getTopologyName());
    }
}

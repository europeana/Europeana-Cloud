package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;

import java.util.*;

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
    return findPostProcessor(taskByTaskState).orElseThrow(() -> new PostProcessingException(
        String.format("No PostProcessor for given topology: '%s'", taskByTaskState.getTopologyName())));
  }

  public Optional<TaskPostProcessor> findPostProcessor(TaskByTaskState taskByTaskState) {
    return Optional.ofNullable(services.get(taskByTaskState.getTopologyName()));
  }
}

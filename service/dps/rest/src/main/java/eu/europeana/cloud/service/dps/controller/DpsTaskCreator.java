package eu.europeana.cloud.service.dps.controller;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class DpsTaskCreator {

  private static final Set<String> ANNOTATION_ADDING_TOPOLOGIES =
      Set.of(TopologiesNames.VALIDATION_TOPOLOGY, TopologiesNames.INDEXING_TOPOLOGY);

  public DpsTask createDpsTask(CreateDpsTaskRequest createTaskRequest, long taskId,String topologyName) {
    var dpsTask = new DpsTask();
    dpsTask.setSource(createTaskRequest.getSource());
    if (createTaskRequest.getResultsProvider() != null) {
      dpsTask.setResultsBatch(
          BatchInfo.builder().providerId(createTaskRequest.getResultsProvider()).batchId(evaluateBatchId(createTaskRequest,taskId, topologyName)).build());
    }
    dpsTask.setParameters(createTaskRequest.getParameters());
    dpsTask.setTaskId(taskId);
    return dpsTask;
  }

  private static String evaluateBatchId(CreateDpsTaskRequest createTaskRequest, long taskId, String topologyName) {
    if(ANNOTATION_ADDING_TOPOLOGIES.contains(topologyName)) {
      return getSourceBatchId(createTaskRequest);
    }else {
      return String.valueOf(taskId);
    }
  }

  private static String getSourceBatchId(CreateDpsTaskRequest createTaskRequest) {
    return ((BatchInfo) createTaskRequest.getSource()).getBatchId();
  }
}

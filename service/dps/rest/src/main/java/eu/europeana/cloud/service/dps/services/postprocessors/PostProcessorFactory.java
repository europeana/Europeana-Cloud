package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

public class PostProcessorFactory {
    private static final Map<String, TaskPostProcessor> services = new HashMap<>();

    @Inject
    public PostProcessorFactory(IndexingPostProcessor indexingPostProcessor, HarvestingPostProcessor harvestingPostProcessor) {
        services.put(TopologiesNames.INDEXING_TOPOLOGY, indexingPostProcessor);
        services.put(TopologiesNames.HTTP_TOPOLOGY, harvestingPostProcessor);
        services.put(TopologiesNames.OAI_TOPOLOGY, harvestingPostProcessor);
    }

    public TaskPostProcessor getPostProcessor(TaskInfo taskInfo) {
        return services.get(taskInfo.getTopologyName());
    }
}

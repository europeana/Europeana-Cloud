package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.SampleSizeForIncrementalHarvestingValidator;
import java.util.HashMap;
import java.util.Map;

public final class DpsTaskValidatorFactory {

  static final String XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS = "xslt_topology";
  static final String ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_DATASETS = "enrichment_topology";
  static final String VALIDATION_TOPOLOGY_TASK_WITH_FILE_DATASETS = "validation_topology";
  static final String NORMALIZATION_TOPOLOGY_TASK_WITH_DATASETS = "normalization_topology";
  static final String OAIPMH_TOPOLOGY_TASK_WITH_REPOSITORY_URL = "oai_topology";
  static final String HTTP_TOPOLOGY_TASK_WITH_REPOSITORY_URL = "http_topology";
  static final String INDEXING_TOPOLOGY_TASK_WITH_DATASETS = "indexing_topology";
  static final String LINK_CHECKING_TASK_WITH_DATASETS = "linkcheck_topology";
  public static final String DEPUBLICATION_TASK = "depublication_topology";
  static final String MEDIA_TOPOLOGY_TASK_WITH_DATASETS = "media_topology";

  private static final Map<String, DpsTaskValidator> taskValidatorMap = buildTaskValidatorMap();

  private DpsTaskValidatorFactory() {
  }

  public static DpsTaskValidator createValidatorForTaskType(String taskType) {
    return taskValidatorMap.get(taskType);
  }

  private static Map<String, DpsTaskValidator> buildTaskValidatorMap() {
    Map<String, DpsTaskValidator> taskValidatorMap = new HashMap<>();

    taskValidatorMap.put(XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS, new DpsTaskValidator("DataSet validator for XSLT Topology")
        .withParameter(PluginParameterKeys.XSLT_URL)
        .withDefinedMCSInput()
        .withDefinedMCSOutput()
        );

    taskValidatorMap.put(OAIPMH_TOPOLOGY_TASK_WITH_REPOSITORY_URL,
        new DpsTaskValidator("RepositoryUrl validator for OAI-PMH Topology")
            .withParameter(PluginParameterKeys.HARVEST_DATE)
            .withDefinedOAIInput()
            .withDefinedMCSOutput()
            .withCustomValidator(new SampleSizeForIncrementalHarvestingValidator()));

    taskValidatorMap.put(HTTP_TOPOLOGY_TASK_WITH_REPOSITORY_URL, new DpsTaskValidator("RepositoryUrl validator for HTTP Topology")
        .withParameter(PluginParameterKeys.HARVEST_DATE)
        .withDefinedHttpInput()
        .withDefinedMCSOutput());

    taskValidatorMap.put(VALIDATION_TOPOLOGY_TASK_WITH_FILE_DATASETS,
        new DpsTaskValidator("DataSet validator for Validation Topology")
            .withDefinedMCSInput()
            .withDefinedMCSOutput()
            .withParameter(PluginParameterKeys.SCHEMA_NAME)
            );

    taskValidatorMap.put(NORMALIZATION_TOPOLOGY_TASK_WITH_DATASETS,
        new DpsTaskValidator("DataSet validator for Normalization Topology")
            .withDefinedMCSInput()
            .withDefinedMCSOutput());

    taskValidatorMap.put(ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_DATASETS,
        new DpsTaskValidator("DataSet validator for Enrichment Topology")
            .withDefinedMCSInput()
            .withDefinedMCSOutput()
            );

    taskValidatorMap.put(INDEXING_TOPOLOGY_TASK_WITH_DATASETS, new DpsTaskValidator("DataSet validator for Indexing Topology")
        .withDefinedMCSInput()
        .withDefinedMCSOutput()
        .withParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE,
            TargetIndexingDatabase.getTargetIndexingDatabaseValues())
        .withParameter(PluginParameterKeys.METIS_DATASET_ID)
        .withParameter(PluginParameterKeys.HARVEST_DATE));

    taskValidatorMap.put(LINK_CHECKING_TASK_WITH_DATASETS, new DpsTaskValidator("DataSet validator for Link checking Topology")
        .withDefinedMCSInput()
        .withNoOutput());

    taskValidatorMap.put(DEPUBLICATION_TASK,
        new DpsTaskValidator("Task validator for Depublication Topology with records list")
            .withNoOutput()
            .withDefinedDepublicationInput()
            .withParameter(PluginParameterKeys.METIS_DATASET_ID)
            .withParameter(PluginParameterKeys.DEPUBLICATION_REASON));

    taskValidatorMap.put(MEDIA_TOPOLOGY_TASK_WITH_DATASETS, new DpsTaskValidator("DataSet validator for Media Topology")
        .withDefinedMCSInput()
        .withDefinedMCSOutput() );

    return taskValidatorMap;
  }

}

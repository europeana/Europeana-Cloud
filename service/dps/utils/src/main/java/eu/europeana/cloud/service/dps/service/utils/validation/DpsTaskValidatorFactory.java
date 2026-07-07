package eu.europeana.cloud.service.dps.service.utils.validation;

import static eu.europeana.cloud.service.dps.service.utils.validation.InputDataValueType.NO_DATA;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.SampleSizeForIncrementalHarvestingValidator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class DpsTaskValidatorFactory {

  private static final DpsTaskValidator ALWAYS_FAIL_VALIDATOR =
      new DpsTaskValidator()
          .withParameter(
              "parameterNameThatWillNeverHappen",
              Collections.singletonList("parameterValueThatWillNeverHappen"));

  static final String XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS = "xslt_topology_dataset_urls";
  static final String ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_DATASETS = "enrichment_topology_dataset_urls";
  static final String VALIDATION_TOPOLOGY_TASK_WITH_FILE_DATASETS = "validation_topology_dataset_urls";
  static final String NORMALIZATION_TOPOLOGY_TASK_WITH_DATASETS = "normalization_topology_dataset_urls";
  static final String OAIPMH_TOPOLOGY_TASK_WITH_REPOSITORY_URL = "oai_topology_repository_urls";
  static final String HTTP_TOPOLOGY_TASK_WITH_REPOSITORY_URL = "http_topology_repository_urls";
  static final String INDEXING_TOPOLOGY_TASK_WITH_DATASETS = "indexing_topology_dataset_urls";
  static final String LINK_CHECKING_TASK_WITH_DATASETS = "linkcheck_topology_dataset_urls";
  public static final String DEPUBLICATION_TASK = "depublication_topology_input";
  static final String MEDIA_TOPOLOGY_TASK_WITH_DATASETS = "media_topology_dataset_urls";

  private static final Map<String, DpsTaskValidator> taskValidatorMap = buildTaskValidatorMap();

  private DpsTaskValidatorFactory() {
  }

  public static DpsTaskValidator createValidatorForTaskType(String taskType) {
    DpsTaskValidator taskValidator = taskValidatorMap.get(taskType);
    return (taskValidator != null ? taskValidator : ALWAYS_FAIL_VALIDATOR);
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

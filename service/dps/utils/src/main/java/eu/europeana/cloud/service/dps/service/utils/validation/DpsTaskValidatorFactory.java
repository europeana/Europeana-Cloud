package eu.europeana.cloud.service.dps.service.utils.validation;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;
import static eu.europeana.cloud.service.dps.service.utils.validation.InputDataValueType.NO_DATA;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.FullyDefinedInputRevisionValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.SampleSizeForIncrementalHarvestingValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.SingleOutputDatasetValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.SingleRepositoryValidator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class DpsTaskValidatorFactory {

  private static final DpsTaskValidator ALWAYS_FAIL_VALIDATOR =
      new DpsTaskValidator()
          .withParameter(
              "parameterNameThatWillNeverHappen",
              Collections.singletonList("parameterValueThatWillNeverHappen"));

  static final String XSLT_TOPOLOGY_TASK_WITH_FILE_URLS = "xslt_topology_file_urls";
  static final String XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS = "xslt_topology_dataset_urls";
  static final String ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_URLS = "enrichment_topology_file_urls";
  static final String ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_DATASETS = "enrichment_topology_dataset_urls";
  static final String VALIDATION_TOPOLOGY_TASK_WITH_FILE_URLS = "validation_topology_file_urls";
  static final String VALIDATION_TOPOLOGY_TASK_WITH_FILE_DATASETS = "validation_topology_dataset_urls";

  static final String NORMALIZATION_TOPOLOGY_TASK_WITH_FILE_URLS = "normalization_topology_file_urls";
  static final String NORMALIZATION_TOPOLOGY_TASK_WITH_DATASETS = "normalization_topology_dataset_urls";

  static final String OAIPMH_TOPOLOGY_TASK_WITH_REPOSITORY_URL = "oai_topology_repository_urls";
  static final String HTTP_TOPOLOGY_TASK_WITH_REPOSITORY_URL = "http_topology_repository_urls";
  static final String INDEXING_TOPOLOGY_TASK_WITH_FILE_URLS = "indexing_topology_file_urls";
  static final String INDEXING_TOPOLOGY_TASK_WITH_DATASETS = "indexing_topology_dataset_urls";

  static final String LINK_CHECKING_TOPOLOGY_TASK_WITH_FILE_URLS = "linkcheck_topology_file_urls";
  static final String LINK_CHECKING_TASK_WITH_DATASETS = "linkcheck_topology_dataset_urls";

  static final String DEPUBLICATION_TASK_FOR_DATASET = "depublication_topology_metis_dataset_id";
  static final String DEPUBLICATION_TASK_FOR_RECORDS = "depublication_topology_record_ids_to_depublish";

  static final String MEDIA_TOPOLOGY_TASK_WITH_FILE_URLS = "media_topology_file_urls";
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

    taskValidatorMap.put(XSLT_TOPOLOGY_TASK_WITH_FILE_URLS, new DpsTaskValidator("FileUrl validator for XSLT Topology")
        .withParameter(PluginParameterKeys.XSLT_URL)
        .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
        .withOptionalOutputRevision()
        .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS, new DpsTaskValidator("DataSet validator for XSLT Topology")
        .withParameter(PluginParameterKeys.XSLT_URL)
        .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
        .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
        .withOptionalOutputRevision()
        .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(OAIPMH_TOPOLOGY_TASK_WITH_REPOSITORY_URL,
        new DpsTaskValidator("RepositoryUrl validator for OAI-PMH Topology")
            .withParameter(PluginParameterKeys.PROVIDER_ID)
            .withParameter(PluginParameterKeys.HARVEST_DATE)
            .withDataEntry(REPOSITORY_URLS.name(), InputDataValueType.LINK_TO_EXTERNAL_URL)
            .withOptionalOutputRevision()
            .withCustomValidator(new SingleRepositoryValidator())
            .withCustomValidator(new SingleOutputDatasetValidator())
            .withCustomValidator(new SampleSizeForIncrementalHarvestingValidator()));

    taskValidatorMap.put(HTTP_TOPOLOGY_TASK_WITH_REPOSITORY_URL, new DpsTaskValidator("RepositoryUrl validator for HTTP Topology")
        .withParameter(PluginParameterKeys.PROVIDER_ID)
        .withParameter(PluginParameterKeys.HARVEST_DATE)
        .withDataEntry(REPOSITORY_URLS.name(), InputDataValueType.LINK_TO_EXTERNAL_URL)
        .withOptionalOutputRevision());

    taskValidatorMap.put(VALIDATION_TOPOLOGY_TASK_WITH_FILE_URLS,
        new DpsTaskValidator("FileUrl validator for Validation Topology")
            .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
            .withAnyOutputRevision()
            .withParameter(PluginParameterKeys.SCHEMA_NAME)
            .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(VALIDATION_TOPOLOGY_TASK_WITH_FILE_DATASETS,
        new DpsTaskValidator("DataSet validator for Validation Topology")
            .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
            .withAnyOutputRevision()
            .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
            .withParameter(PluginParameterKeys.SCHEMA_NAME)
            .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(NORMALIZATION_TOPOLOGY_TASK_WITH_FILE_URLS,
        new DpsTaskValidator("FileUrl validator for Normalization Topology")
            .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
            .withOptionalOutputRevision()
            .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(NORMALIZATION_TOPOLOGY_TASK_WITH_DATASETS,
        new DpsTaskValidator("DataSet validator for Normalization Topology")
            .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
            .withOptionalOutputRevision()
            .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
            .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_URLS,
        new DpsTaskValidator("FileUrl validator for Enrichment Topology")
            .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
            .withOptionalOutputRevision()
            .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_DATASETS,
        new DpsTaskValidator("DataSet validator for Enrichment Topology")
            .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
            .withOptionalOutputRevision()
            .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
            .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(INDEXING_TOPOLOGY_TASK_WITH_FILE_URLS, new DpsTaskValidator("FileUrl validator for Indexing Topology")
        .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
        .withOptionalOutputRevision()
        .withParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE,
            TargetIndexingDatabase.getTargetIndexingDatabaseValues())
        .withParameter(PluginParameterKeys.METIS_DATASET_ID)
        .withParameter(PluginParameterKeys.HARVEST_DATE)
        .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(INDEXING_TOPOLOGY_TASK_WITH_DATASETS, new DpsTaskValidator("DataSet validator for Indexing Topology")
        .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
        .withOptionalOutputRevision()
        .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
        .withParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE,
            TargetIndexingDatabase.getTargetIndexingDatabaseValues())
        .withParameter(PluginParameterKeys.METIS_DATASET_ID)
        .withParameter(PluginParameterKeys.HARVEST_DATE)
        .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(LINK_CHECKING_TOPOLOGY_TASK_WITH_FILE_URLS,
        new DpsTaskValidator("FileUrl validator for Link checking Topology")
            .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
            .withOptionalOutputRevision()
            .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(LINK_CHECKING_TASK_WITH_DATASETS, new DpsTaskValidator("DataSet validator for Link checking Topology")
        .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
        .withOptionalOutputRevision()
        .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
        .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(DEPUBLICATION_TASK_FOR_DATASET,
        new DpsTaskValidator("Task validator for Depublication Topology with dataset id")
            .withDataEntry(null, NO_DATA)
            .withParameter(PluginParameterKeys.METIS_DATASET_ID));

    taskValidatorMap.put(DEPUBLICATION_TASK_FOR_RECORDS,
        new DpsTaskValidator("Task validator for Depublication Topology with records list")
            .withDataEntry(null, NO_DATA)
            .withParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH));

    taskValidatorMap.put(MEDIA_TOPOLOGY_TASK_WITH_FILE_URLS, new DpsTaskValidator("FileUrl validator for Media Topology")
        .withParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME)
        .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
        .withOptionalOutputRevision()
        .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    taskValidatorMap.put(MEDIA_TOPOLOGY_TASK_WITH_DATASETS, new DpsTaskValidator("DataSet validator for Media Topology")
        .withParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME)
        .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
        .withOptionalOutputRevision()
        .withCustomValidator(new FullyDefinedInputRevisionValidator()));

    return taskValidatorMap;
  }

}

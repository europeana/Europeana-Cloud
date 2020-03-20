package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.InputDataValueType;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;

import java.util.HashMap;
import java.util.Map;

import static eu.europeana.cloud.service.dps.InputDataType.*;

public class DpsTaskValidatorFactory {

    private final static DpsTaskValidator EMPTY_VALIDATOR = new DpsTaskValidator();
    private final static String XSLT_TOPOLOGY_TASK_WITH_FILE_URLS = "xslt_topology_file_urls";
    private final static String XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS = "xslt_topology_dataset_urls";
    private final static String ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_URLS = "enrichment_topology_file_urls";
    private final static String ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_DATASETS = "enrichment_topology_dataset_urls";
    private final static String VALIDATION_TOPOLOGY_TASK_WITH_FILE_URLS = "validation_topology_file_urls";
    private final static String VALIDATION_TOPOLOGY_TASK_WITH_FILE_DATASETS = "validation_topology_dataset_urls";

    private final static String IC_TOPOLOGY_TASK_WITH_FILE_URLS = "ic_topology_file_urls";
    private final static String IC_TOPOLOGY_TASK_WITH_DATASETS = "ic_topology_dataset_urls";

    private final static String NORMALIZATION_TOPOLOGY_TASK_WITH_FILE_URLS = "normalization_topology_file_urls";
    private final static String NORMALIZATION_TOPOLOGY_TASK_WITH_DATASETS = "normalization_topology_dataset_urls";

    private final static String OAIPMH_TOPOLOGY_TASK_WITH_REPOSITORY_URL = "oai_topology_repository_urls";
    private final static String HTTP_TOPOLOGY_TASK_WITH_REPOSITORY_URL = "http_topology_repository_urls";
    private final static String JP2_MIME_TYPE = "image/jp2";
    private final static String INDEXING_TOPOLOGY_TASK_WITH_FILE_URLS = "indexing_topology_file_urls";
    private final static String INDEXING_TOPOLOGY_TASK_WITH_DATASETS = "indexing_topology_dataset_urls";


    private final static String LINK_CHECKING_TOPOLOGY_TASK_WITH_FILE_URLS = "linkcheck_topology_file_urls";
    private final static String LINK_CHECKING_TASK_WITH_DATASETS = "linkcheck_topology_dataset_urls";

    private static Map<String, DpsTaskValidator> taskValidatorMap = buildTaskValidatorMap();

    private DpsTaskValidatorFactory() {
    }

    public static DpsTaskValidator createValidator(String taskType) {
        DpsTaskValidator taskValidator = taskValidatorMap.get(taskType);
        return (taskValidator != null ? taskValidator : EMPTY_VALIDATOR);
    }

    private static Map<String, DpsTaskValidator> buildTaskValidatorMap() {
        Map<String, DpsTaskValidator> taskValidatorMap = new HashMap<>();

        taskValidatorMap.put(XSLT_TOPOLOGY_TASK_WITH_FILE_URLS, new DpsTaskValidator("FileUrl validator for XSLT Topology")
                .withParameter(PluginParameterKeys.XSLT_URL)
                .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
                .withOptionalOutputRevision());

        taskValidatorMap.put(XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS, new DpsTaskValidator("DataSet validator for XSLT Topology")
                .withParameter(PluginParameterKeys.XSLT_URL)
                .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
                .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
                .withOptionalOutputRevision());

        taskValidatorMap.put(IC_TOPOLOGY_TASK_WITH_FILE_URLS, new DpsTaskValidator("FileUrl validator for IC Topology")
                .withParameter(PluginParameterKeys.MIME_TYPE)
                .withParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, JP2_MIME_TYPE)
                .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
                .withOptionalOutputRevision());

        taskValidatorMap.put(IC_TOPOLOGY_TASK_WITH_DATASETS, new DpsTaskValidator("DataSet validator for IC Topology")
                .withParameter(PluginParameterKeys.MIME_TYPE)
                .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
                .withParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, JP2_MIME_TYPE)
                .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
                .withOptionalOutputRevision());

        taskValidatorMap.put(OAIPMH_TOPOLOGY_TASK_WITH_REPOSITORY_URL, new DpsTaskValidator("RepositoryUrl validator for OAI-PMH Topology")
                .withParameter(PluginParameterKeys.PROVIDER_ID)
                .withDataEntry(REPOSITORY_URLS.name(), InputDataValueType.LINK_TO_EXTERNAL_URL)
                .withOptionalOutputRevision());

        taskValidatorMap.put(HTTP_TOPOLOGY_TASK_WITH_REPOSITORY_URL, new DpsTaskValidator("RepositoryUrl validator for HTTP Topology")
                .withParameter(PluginParameterKeys.PROVIDER_ID)
                .withDataEntry(REPOSITORY_URLS.name(), InputDataValueType.LINK_TO_EXTERNAL_URL)
                .withOptionalOutputRevision());

        taskValidatorMap.put(VALIDATION_TOPOLOGY_TASK_WITH_FILE_URLS, new DpsTaskValidator("FileUrl validator for Validation Topology")
                .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
                .withAnyOutputRevision()
                .withParameter(PluginParameterKeys.SCHEMA_NAME));

        taskValidatorMap.put(VALIDATION_TOPOLOGY_TASK_WITH_FILE_DATASETS, new DpsTaskValidator("DataSet validator for Validation Topology")
                .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
                .withAnyOutputRevision()
                .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
                .withParameter(PluginParameterKeys.SCHEMA_NAME));


        taskValidatorMap.put(NORMALIZATION_TOPOLOGY_TASK_WITH_FILE_URLS, new DpsTaskValidator("FileUrl validator for Normalization Topology")
                .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
                .withOptionalOutputRevision());

        taskValidatorMap.put(NORMALIZATION_TOPOLOGY_TASK_WITH_DATASETS, new DpsTaskValidator("DataSet validator for Normalization Topology")
                .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
                .withOptionalOutputRevision()
                .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET));


        taskValidatorMap.put(ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_URLS, new DpsTaskValidator("FileUrl validator for Enrichment Topology")
                .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
                .withOptionalOutputRevision());

        taskValidatorMap.put(ENRICHMENT_TOPOLOGY_TASK_WITH_FILE_DATASETS, new DpsTaskValidator("DataSet validator for Enrichment Topology")
                .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
                .withOptionalOutputRevision()
                .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET));

        taskValidatorMap.put(INDEXING_TOPOLOGY_TASK_WITH_FILE_URLS, new DpsTaskValidator("FileUrl validator for Indexing Topology")
                .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
                .withOptionalOutputRevision()
                .withParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.getTargetIndexingDatabaseValues())
                .withParameter(PluginParameterKeys.METIS_DATASET_ID));

        taskValidatorMap.put(INDEXING_TOPOLOGY_TASK_WITH_DATASETS, new DpsTaskValidator("DataSet validator for Indexing Topology")
                .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
                .withOptionalOutputRevision()
                .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET)
                .withParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.getTargetIndexingDatabaseValues())
                .withParameter(PluginParameterKeys.METIS_DATASET_ID));


        taskValidatorMap.put(LINK_CHECKING_TOPOLOGY_TASK_WITH_FILE_URLS, new DpsTaskValidator("FileUrl validator for Link checking Topology")
                .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE)
                .withOptionalOutputRevision());

        taskValidatorMap.put(LINK_CHECKING_TASK_WITH_DATASETS, new DpsTaskValidator("DataSet validator for Link checking Topology")
                .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
                .withOptionalOutputRevision()
                .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET));


        return taskValidatorMap;
    }
}

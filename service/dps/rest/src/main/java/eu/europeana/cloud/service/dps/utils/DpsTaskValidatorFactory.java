package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.InputDataValueType;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;

public class DpsTaskValidatorFactory {

    private static final DpsTaskValidator EMPTY_VALIDATOR = new DpsTaskValidator();
    private final static String XSLT_TOPOLOGY_TASK_WITH_FILE_URLS = "xslt_topology_file_urls";
    private final static String XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS = "xslt_topology_dataset_urls";

    private final static String IC_TOPOLOGY_TASK_WITH_FILE_URLS = "ic_topology_file_urls";
    private final static String IC_TOPOLOGY_TASK_WITH_DATASETS = "ic_topology_dataset_urls";
    private final static String JP2_MIME_TYPE = "image/jp2";

    public static DpsTaskValidator createValidator(String taskType) {
        if (taskType.equalsIgnoreCase(XSLT_TOPOLOGY_TASK_WITH_FILE_URLS)) {
            return new DpsTaskValidator("FileUrl validator for XSLT Topology")
                    .withParameter(PluginParameterKeys.XSLT_URL)
                    .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE);
        } else if (taskType.equalsIgnoreCase(XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS)) {
            return new DpsTaskValidator("DataSet validator for XSLT Topology")
                    .withParameter(PluginParameterKeys.XSLT_URL)
                    .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
                    .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET);
        } else if (taskType.equalsIgnoreCase(IC_TOPOLOGY_TASK_WITH_FILE_URLS)) {
            return new DpsTaskValidator("FileUrl validator for IC Topology")
                    .withParameter(PluginParameterKeys.MIME_TYPE)
                    .withParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, JP2_MIME_TYPE)
                    .withDataEntry(FILE_URLS.name(), InputDataValueType.LINK_TO_FILE);
        } else if (taskType.equalsIgnoreCase(IC_TOPOLOGY_TASK_WITH_DATASETS)) {
            return new DpsTaskValidator("DataSet validator for IC Topology")
                    .withParameter(PluginParameterKeys.MIME_TYPE)
                    .withParameter(PluginParameterKeys.REPRESENTATION_NAME)
                    .withParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, JP2_MIME_TYPE)
                    .withDataEntry(DATASET_URLS.name(), InputDataValueType.LINK_TO_DATASET);
        } else {
            return EMPTY_VALIDATOR;
        }
    }
}

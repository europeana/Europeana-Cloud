package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.InputDataValueType;

public class DpsTaskValidatorFactory {

    private static final DpsTaskValidator EMPTY_VALIDATOR = new DpsTaskValidator();
    private final static String XSLT_TOPOLOGY_TASK_WITH_FILE_URLS = "xslt_topology_file_urls";
    private final static String XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS = "xslt_topology_dataset_urls";
    
    private final static String IC_TOPOLOGY_TASK_WITH_FILE_URLS = "ic_topology_file_urls";
    private final static String IC_TOPOLOGY_TASK_WITH_DATASETS = "ic_topology_dataset_urls";
    
    public static DpsTaskValidator createValidator(String taskType) {
        if (taskType.equalsIgnoreCase(XSLT_TOPOLOGY_TASK_WITH_FILE_URLS)) {
            DpsTaskValidator validator = new DpsTaskValidator("FileUrl validator for XSLT Topology")
                    .withParameter(PluginParameterKeys.XSLT_URL)
                    .withDataEntry(PluginParameterKeys.FILE_URLS, InputDataValueType.LINK_TO_FILE);
            return validator;
        } else if (taskType.equalsIgnoreCase(XSLT_TOPOLOGY_TASK_WITH_FILE_DATASETS)) {
            DpsTaskValidator validator = new DpsTaskValidator("DataSet validator for XSLT Topology")
                    .withParameter(PluginParameterKeys.XSLT_URL)
            .withDataEntry(PluginParameterKeys.DATASET_URLS, InputDataValueType.LINK_TO_DATASET);
            return validator;
        } else if (taskType.equalsIgnoreCase(IC_TOPOLOGY_TASK_WITH_FILE_URLS)) {
            DpsTaskValidator validator = new DpsTaskValidator("FileUrl validator for IC Topology")
                    .withParameter(PluginParameterKeys.MIME_TYPE)
                    .withParameter(PluginParameterKeys.OUTPUT_MIME_TYPE)
                    .withDataEntry(PluginParameterKeys.FILE_URLS, InputDataValueType.LINK_TO_FILE);
            return validator;
        }else if(taskType.equalsIgnoreCase(IC_TOPOLOGY_TASK_WITH_DATASETS)){
            DpsTaskValidator validator = new DpsTaskValidator("DataSet validator for IC Topology")
                    .withParameter(PluginParameterKeys.MIME_TYPE)
                    .withParameter(PluginParameterKeys.OUTPUT_MIME_TYPE)
                    .withDataEntry(PluginParameterKeys.DATASET_URLS, InputDataValueType.LINK_TO_DATASET);
            return validator;
        } else {
            return EMPTY_VALIDATOR;
        }
    }
}

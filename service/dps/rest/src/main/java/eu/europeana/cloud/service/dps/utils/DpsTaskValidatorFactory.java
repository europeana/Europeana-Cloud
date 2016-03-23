package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.InputDataValueType;

import java.util.ArrayList;
import java.util.List;

public class DpsTaskValidatorFactory {

    private static final DpsTaskValidator EMPTY_VALIDATOR = new DpsTaskValidator();
    private final static String XSLT_TOPOLOGY_NAME = "xslt_topology";
    private final static String IC_TOPOLOGY_NAME = "ic_topology";

    public static List<DpsTaskValidator> createValidators(String topologyName) {
        List<DpsTaskValidator> validators = new ArrayList<>();
        if (topologyName.equals(XSLT_TOPOLOGY_NAME)) {
            DpsTaskValidator validator = new DpsTaskValidator()
                    .withDataEntry(PluginParameterKeys.FILE_URLS, InputDataValueType.LINK_TO_FILE)
                    .withParameter(PluginParameterKeys.XSLT_URL)
                    .withParameter(PluginParameterKeys.TASK_SUBMITTER_NAME);
            validators.add(validator);
            return validators;
        } else if (topologyName.equals(IC_TOPOLOGY_NAME)) {
            DpsTaskValidator validator_for_file_urls = new DpsTaskValidator("FileUrl validator")
                    .withDataEntry(PluginParameterKeys.FILE_URLS, InputDataValueType.LINK_TO_FILE)
                    .withParameter(PluginParameterKeys.TASK_SUBMITTER_NAME);
            validators.add(validator_for_file_urls);
            //
            DpsTaskValidator validator_for_dataset_urls = new DpsTaskValidator("DataSet validator")
                    .withDataEntry(PluginParameterKeys.DATASET_URL, InputDataValueType.LINK_TO_DATASET)
                    .withParameter(PluginParameterKeys.MIME_TYPE)
                    .withParameter(PluginParameterKeys.OUTPUT_MIME_TYPE)
                    .withParameter(PluginParameterKeys.TASK_SUBMITTER_NAME);
            validators.add(validator_for_dataset_urls);

            return validators;
        } else {
            validators.add(EMPTY_VALIDATOR);
            return validators;
        }
    }
}

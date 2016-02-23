package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.InputDataValueType;

public class DpsTaskValidatorFactory {

    private static final DpsTaskValidator EMPTY_VALIDATOR = new DpsTaskValidator();
    private final static String XSLT_TOPOLOGY_NAME = "xslt_topology";
    private final static String IC_TOPOLOGY_NAME = "ic_topology";

    public static DpsTaskValidator createValidator(String topologyName) {
        if (topologyName.equals(XSLT_TOPOLOGY_NAME)) {
            DpsTaskValidator validator = new DpsTaskValidator()
                    .withDataEntry(PluginParameterKeys.FILE_URLS, InputDataValueType.LINK_TO_FILE)
                    .withParameter(PluginParameterKeys.XSLT_URL)
                    .withParameter(PluginParameterKeys.TASK_SUBMITTER_NAME);
            return validator;
        } else if (topologyName.equals(IC_TOPOLOGY_NAME)) {
            DpsTaskValidator validator = new DpsTaskValidator()
                    .withDataEntry(PluginParameterKeys.FILE_URLS, InputDataValueType.LINK_TO_FILE)
                    .withParameter(PluginParameterKeys.TASK_SUBMITTER_NAME);
            return validator;
        } else {
            return EMPTY_VALIDATOR;
        }
    }
}

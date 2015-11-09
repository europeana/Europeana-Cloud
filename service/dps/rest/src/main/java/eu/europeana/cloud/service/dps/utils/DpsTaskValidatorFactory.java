package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.InputDataValueType;

public class DpsTaskValidatorFactory {

    private static final DpsTaskValidator EMPTY_VALIDATOR = new DpsTaskValidator();

    public static DpsTaskValidator createValidator(String topologyName) {
        if (topologyName.equals("xsltTopology")) {
            DpsTaskValidator validator = new DpsTaskValidator().withParameter("XSLT_URL");
            return validator;
        } else if (topologyName.equals("icTopology")) {
            DpsTaskValidator validator = new DpsTaskValidator()
                    .withDataEntry("FILE_URLS", InputDataValueType.LINK_TO_FILE)
                    .withParameter("OUTPUT_MIME_TYPE");
            return validator;
        } else {
            return EMPTY_VALIDATOR;
        }
    }
}

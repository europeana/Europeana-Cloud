package eu.europeana.cloud.service.dps.service.utils.validation;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pwozniak on 5/29/18
 */
public enum TargetIndexingEnvironment {

    DEFAULT,
    ALTERNATIVE;

    public static List<String> getStringValues() {
        List<String> result = new ArrayList<>();
        for (TargetIndexingEnvironment env :
                TargetIndexingEnvironment.values()) {
            result.add(env.toString());
        }
        return result;
    }
}

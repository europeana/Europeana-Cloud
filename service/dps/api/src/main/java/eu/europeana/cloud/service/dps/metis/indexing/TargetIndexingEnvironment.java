package eu.europeana.cloud.service.dps.metis.indexing;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pwozniak on 5/29/18
 */
@Deprecated
public enum TargetIndexingEnvironment {
    DEFAULT,
    ALTERNATIVE;

    public static List<String> getStringValues() {
        List<String> result = new ArrayList<>(TargetIndexingEnvironment.values().length);
        for (TargetIndexingEnvironment env :
                TargetIndexingEnvironment.values()) {
            result.add(env.toString());
        }
        return result;
    }
}

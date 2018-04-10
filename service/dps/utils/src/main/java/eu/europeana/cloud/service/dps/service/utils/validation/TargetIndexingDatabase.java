package eu.europeana.cloud.service.dps.service.utils.validation;

import java.util.ArrayList;
import java.util.List;

public enum TargetIndexingDatabase {
    PUBLISH,
    PREVIEW;
    private static List<String> values = initializeTargetIndexingDatabaseValues();

    public static List<String> getTargetIndexingDatabaseValues() {
        return values;
    }

    private static List<String> initializeTargetIndexingDatabaseValues() {
        List<String> values = new ArrayList<>();
        for (TargetIndexingDatabase targetIndexingDatabase : TargetIndexingDatabase.values())
            values.add(targetIndexingDatabase.toString());
        return values;
    }
}

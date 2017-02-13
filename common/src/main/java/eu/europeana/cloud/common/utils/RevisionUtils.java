package eu.europeana.cloud.common.utils;

import eu.europeana.cloud.common.model.Revision;

public class RevisionUtils {
    public static String getRevisionKey(String providerId, String revisionName) {
        return providerId + "_" + revisionName;
    }

    public static String getRevisionKey(Revision revision) {
        return revision.getRevisionProviderId() + "_" + revision.getRevisionName();
    }
}

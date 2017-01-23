package eu.europeana.cloud.common.utils;

import eu.europeana.cloud.common.model.Revision;

/**
 * Created by Tarek on 8/8/2016.
 */
public class RevisionUtils {
    public static String getRevisionKey(String providerId, String revisionName, Long time) {
        return providerId + "_" + revisionName + "_" + time;
    }

    public static String getRevisionKey(Revision revision) {
        if (revision == null)
            throw new IllegalArgumentException("Cannot create key from null revision!");

        return getRevisionKey(revision.getRevisionProviderId(), revision.getRevisionName(), revision.getCreationTimeStamp().getTime());
    }
}

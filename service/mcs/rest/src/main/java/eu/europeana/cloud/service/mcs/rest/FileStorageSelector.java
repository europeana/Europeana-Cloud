package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

import static org.apache.commons.lang3.Validate.notNull;

public class FileStorageSelector {
    private static final Set<String> MIME_TYPES_STORED_IN_DB = ImmutableSet.of("application/xml", "text/xml");

    private FileStorageSelector() {
        throw new UnsupportedOperationException();
    }

    protected static boolean selectStorage(String mimeType) {
        notNull(mimeType);
        return MIME_TYPES_STORED_IN_DB.contains(mimeType) ? true : false;
    }
}
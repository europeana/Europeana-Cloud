package eu.europeana.cloud.mcs.driver;

import static eu.europeana.cloud.common.utils.UrlUtils.removeLastSlash;

/**
 * @author krystian.
 */
public abstract class MCSClient {
    protected final String baseUrl;
    protected static final int DEFAULT_CONNECT_TIMEOUT_IN_MILLIS = 20 * 1000;
    protected static final int DEFAULT_READ_TIMEOUT_IN_MILLIS = 60 * 1000;

    public MCSClient(final String baseUrl) {
        this.baseUrl = removeLastSlash(baseUrl);
    }
}

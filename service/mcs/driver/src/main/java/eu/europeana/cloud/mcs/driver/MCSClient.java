package eu.europeana.cloud.mcs.driver;

import static eu.europeana.cloud.common.utils.UrlUtils.removeLastSlash;

/**
 * @author krystian.
 */
public abstract class MCSClient {
    protected final String baseUrl;

    public MCSClient(final String baseUrl) {
        this.baseUrl = removeLastSlash(baseUrl);
    }
}

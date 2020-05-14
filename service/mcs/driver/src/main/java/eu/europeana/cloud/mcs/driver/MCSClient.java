package eu.europeana.cloud.mcs.driver;

import net.iharder.Base64;

import static eu.europeana.cloud.common.utils.UrlUtils.removeLastSlash;

/**
 * @author krystian.
 */
public abstract class MCSClient {
    protected static final int DEFAULT_CONNECT_TIMEOUT_IN_MILLIS = 20 * 1000;
    protected static final int DEFAULT_READ_TIMEOUT_IN_MILLIS = 60 * 1000;
    public static final String AUTHORIZATION_KEY = "Authorization";
    public static final String AUTHORIZATION_VALUE_PREFIX = "Basic ";

    protected final String baseUrl;

    public MCSClient(final String baseUrl) {
        this.baseUrl = removeLastSlash(baseUrl);
    }

    public static String getAuthorisationValue(String user, String password) {
        String userPasswordToken = user + ":" + password;
        return AUTHORIZATION_VALUE_PREFIX + Base64.encodeBytes(userPasswordToken.getBytes());
    }

}

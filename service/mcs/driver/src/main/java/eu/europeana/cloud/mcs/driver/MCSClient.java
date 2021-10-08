package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import net.iharder.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

import static eu.europeana.cloud.common.utils.UrlUtils.removeLastSlash;

/**
 * @author krystian.
 */
public abstract class MCSClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(MCSClient.class);

    protected static final int DEFAULT_CONNECT_TIMEOUT_IN_MILLIS = 20 * 1000;
    protected static final int DEFAULT_READ_TIMEOUT_IN_MILLIS = 60 * 1000;
    public static final String AUTHORIZATION_KEY = "Authorization";
    public static final String AUTHORIZATION_VALUE_PREFIX = "Basic ";

    protected final String baseUrl;

    public MCSClient(final String baseUrl) {
        this.baseUrl = removeLastSlash(baseUrl);
        LOGGER.info("Creating MCS client with baseUrl: {}", baseUrl);
    }

    public static String getAuthorisationValue(String user, String password) {
        String userPasswordToken = user + ":" + password;
        return AUTHORIZATION_VALUE_PREFIX + Base64.encodeBytes(userPasswordToken.getBytes());
    }

    protected ErrorInfo getErrorInfo(Response response) throws MCSException {
        try {
            LOGGER.error("URI: {} ;Status: {} {}", response.getLocation(), response.getStatusInfo().getStatusCode(), response.getStatusInfo().getReasonPhrase());
            return response.readEntity(ErrorInfo.class);
        } catch (Exception e) {
            LOGGER.error("Unknown error while processing response entity", e);
            throw MCSExceptionProvider.generateException(null);
        }
    }
}

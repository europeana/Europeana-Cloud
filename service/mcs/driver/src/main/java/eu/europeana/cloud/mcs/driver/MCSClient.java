package eu.europeana.cloud.mcs.driver;

import net.iharder.Base64;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import static eu.europeana.cloud.common.utils.UrlUtils.removeLastSlash;

/**
 * Base class for MCS clients
 */
public abstract class MCSClient implements AutoCloseable {
    protected static final int DEFAULT_CONNECT_TIMEOUT_IN_MILLIS = 20 * 1000;
    protected static final int DEFAULT_READ_TIMEOUT_IN_MILLIS = 60 * 1000;
    public static final String AUTHORIZATION_KEY = "Authorization";
    public static final String AUTHORIZATION_VALUE_PREFIX = "Basic ";

    protected final String baseUrl;

    protected final Client client = ClientBuilder.newBuilder()
            .register(JacksonFeature.class)
            .register(MultiPartFeature.class)
            .build();

    public MCSClient(final String baseUrl) {
        this.baseUrl = removeLastSlash(baseUrl);
    }

    public static String getAuthorisationValue(String user, String password) {
        String userPasswordToken = user + ":" + password;
        return AUTHORIZATION_VALUE_PREFIX + Base64.encodeBytes(userPasswordToken.getBytes());
    }
}

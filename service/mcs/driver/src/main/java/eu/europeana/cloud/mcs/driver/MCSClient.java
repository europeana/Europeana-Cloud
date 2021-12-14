package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.filter.ECloudBasicAuthFilter;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import net.iharder.Base64;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.util.function.Supplier;

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

    protected MCSClient(final String baseUrl) {
        this.baseUrl = removeLastSlash(baseUrl);
    }

    public static String getAuthorisationValue(String user, String password) {
        String userPasswordToken = user + ":" + password;
        return AUTHORIZATION_VALUE_PREFIX + Base64.encodeBytes(userPasswordToken.getBytes());
    }

    public void close() {
        client.close();
    }

    /**
     * Client will use provided authorization header for all requests;
     *
     * @param authorizationHeader authorization header value
     */
    @SuppressWarnings("unused")
    public void useAuthorizationHeader(final String authorizationHeader) {
        client.register(new ECloudBasicAuthFilter(authorizationHeader));
    }

    protected void closeResponse(Response response) {
        if (response != null) {
            response.close();
        }
    }

    protected <T> T manageResponse(ResponseParams<T> responseParameters, Supplier<Response> responseSupplier) throws MCSException {
        Response response = responseSupplier.get();
        try {
            response.bufferEntity();
            if (responseParameters.isCodeInValidStatus(response.getStatus())) {
                return readEntityByClass(responseParameters, response);
            }
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        } catch(MCSException | DriverException knownException) {
            throw knownException; //re-throw just created CloudException
        } catch(ProcessingException processingException) {
            String message = String.format("Could not deserialize response with statusCode: %d; message: %s",
                    response.getStatus(), response.readEntity(String.class));
            throw MCSExceptionProvider.createException(message, processingException);
        } catch(Exception otherExceptions) {
            throw MCSExceptionProvider.createException("Other client error", otherExceptions);
        } finally {
            closeResponse(response);
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T readEntityByClass(ResponseParams<T> responseParameters, Response response) throws IOException {
        if(responseParameters.getExpectedClass() == Void.class) {
            return null;
        } else if(responseParameters.getExpectedClass() == Boolean.class) {
            return (T)Boolean.TRUE;
        } else if(responseParameters.getExpectedClass() == URI.class) {
            return (T) response.getLocation();
        } else if(responseParameters.getExpectedClass() == Response.Status.class) {
            return (T) Response.Status.fromStatusCode(response.getStatus());
        } else if(responseParameters.getGenericType() != null) {
            return response.readEntity(responseParameters.getGenericType());
        } else {
            return response.readEntity(responseParameters.getExpectedClass());
        }
    }


    @AllArgsConstructor
    @Getter
    protected static class ResponseParams<T> {
        private Class<T> expectedClass;
        private GenericType<T> genericType;
        private Response.Status[] validStatuses;

        public ResponseParams(Class<T> expectedClass) {
            this(expectedClass, null, new Response.Status[]{Response.Status.OK});
        }

        public ResponseParams(Class<T> expectedClass, Response.Status validStatus) {
            this(expectedClass, null, new Response.Status[]{validStatus});
        }

        public ResponseParams(Class<T> expectedClass, Response.Status[] validStatuses) {
            this(expectedClass, null, validStatuses);
        }

        public ResponseParams(GenericType<T> genericType) {
            this(null, genericType, new Response.Status[]{Response.Status.OK});
        }

        public boolean isCodeInValidStatus(int code) {
            for(Response.Status status: validStatuses) {
                if(status.getStatusCode() == code) {
                    return true;
                }
            }
            return false;
        }
    }



}

package eu.europeana.cloud.mcs.driver.filter;

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import java.io.IOException;

/**
 * Client request filter which will add provided header value as a authorization header to request
 */
public class ECloudBasicAuthFilter implements ClientRequestFilter {

    private String headerValue;

    public ECloudBasicAuthFilter(String headerValue){
        this.headerValue = headerValue;
    }
    
    @Override
    public void filter(ClientRequestContext requestContext) throws IOException {
        requestContext.getHeaders().remove(HttpHeaders.AUTHORIZATION);
        if (!requestContext.getHeaders().containsKey(HttpHeaders.AUTHORIZATION)) {
            requestContext.getHeaders().add(HttpHeaders.AUTHORIZATION, headerValue);
        }
    }
}

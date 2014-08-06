package eu.europeana.cloud.service.aas.authentication.handlers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

/**
 * Authentication should only be done by a request to the correct URI and proper
 * authentication. All requests should fail with a 401 UNAUTHORIZED status code
 * in case the user is not authenticated.
 *
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
public class RestAuthenticationEntryPoint implements AuthenticationEntryPoint {
    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException authException) throws IOException {
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
    }
}

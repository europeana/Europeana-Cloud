package eu.europeana.cloud.service.aas.authentication.handlers;

import eu.europeana.cloud.common.response.ErrorInfo;

import java.io.IOException;
import java.io.PrintWriter;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

/**
 * Authentication should only be done by a request to the correct URI and proper authentication. All requests should fail with a
 * 401 UNAUTHORIZED status code in case the user is not authenticated.
 *
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
public class CloudAuthenticationEntryPoint implements AuthenticationEntryPoint {

  @Override
  public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException authException)
      throws IOException {
    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    response.setHeader("Content-Type", "application/xml");
    writeErrorInfo(
        response.getWriter(),
        authException);
  }

  private void writeErrorInfo(PrintWriter writer, Exception exception) {
    JAXBContext contextObj;
    try {
      contextObj = JAXBContext.newInstance(ErrorInfo.class);
      Marshaller marshallerObj = contextObj.createMarshaller();
      marshallerObj.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      ErrorInfo e = new ErrorInfo("ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION", exception.getMessage());
      marshallerObj.marshal(e, writer);
    } catch (JAXBException e) {
      writer.println("<errorInfo><errorCode>OTHER</errorCode><details>" + e.getMessage() + "</details></errorInfo>");
    }
  }
}

package eu.europeana.cloud.client.uis.rest;

import eu.europeana.cloud.common.response.ErrorInfo;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.MessageBodyReader;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

public class ErrorInfoMessageBodyReader
        implements MessageBodyReader<ErrorInfo> {

    @Override
    public boolean isReadable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return type == ErrorInfo.class;
    }

    @Override
    public ErrorInfo readFrom(Class<ErrorInfo> type,
                           Type genericType,
                           Annotation[] annotations, MediaType mediaType,
                           MultivaluedMap<String, String> httpHeaders,
                           InputStream entityStream)
            throws IOException, WebApplicationException {

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(ErrorInfo.class);
            return (ErrorInfo) jaxbContext.createUnmarshaller()
                    .unmarshal(entityStream);
        } catch (JAXBException jaxbException) {
            throw new ProcessingException("Error deserializing a MyBean.",
                    jaxbException);
        }
    }
}
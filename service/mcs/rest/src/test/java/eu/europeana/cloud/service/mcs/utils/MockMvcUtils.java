package eu.europeana.cloud.service.mcs.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMultipartHttpServletRequestBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;

public class MockMvcUtils {

    public static final MediaType MEDIA_TYPE_APPLICATION_SVG_XML = MediaType.parseMediaType("application/svg+xml");

    public static String toJson(Object object) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writer().writeValueAsString(object);
    }


    public static String responseContentAsString(ResultActions response) throws UnsupportedEncodingException {
        return response.andReturn().getResponse().getContentAsString();
    }

    public static byte[] responseContentAsByteArray(ResultActions response) {
        return response.andReturn().getResponse().getContentAsByteArray();
    }

    public static ResultSlice<Representation> responseContentAsRepresentationResultSlice(ResultActions response) throws IOException {
        return responseContent(response, new TypeReference<ResultSlice<Representation>>() {
        });
    }

    public static ResultSlice<CloudTagsResponse> responseContentAsCloudTagResultSlice(ResultActions response) throws IOException {
        return responseContent(response, new TypeReference<ResultSlice<CloudTagsResponse>>() {
        });
    }

    public static ResultSlice<CloudIdAndTimestampResponse> responseContentAsCloudIdAndTimestampResultSlice(ResultActions response) throws IOException {
        return responseContent(response, new TypeReference<ResultSlice<CloudIdAndTimestampResponse>>() {
        });
    }

    public static ErrorInfo responseContentAsErrorInfo(ResultActions response) throws IOException {
        return responseContent(response, ErrorInfo.class);
    }

    public static <T> T responseContent(ResultActions response, Class<T> aClass) throws IOException {
        return new ObjectMapper().readValue(response.andReturn().getResponse().getContentAsString(), aClass);
    }


    public static <T> T responseContent(ResultActions response, Class<T> aClass, MediaType mediaType) throws IOException {
        if(mediaType.equals( MediaType.APPLICATION_XML)) {
            return xmlResponseContent(response, aClass);
        } else if(mediaType.equals( MediaType.APPLICATION_JSON)) {
            return responseContent(response, aClass);
        }else {
            throw new IllegalArgumentException("mediaType="+mediaType.getType());
        }
    }

    public static <T> T xmlResponseContent(ResultActions response, Class<T> aClass) throws IOException {
        return new XmlMapper().readValue(response.andReturn().getResponse().getContentAsString(), aClass);
    }


    private static  <T> T responseContent(ResultActions response, TypeReference<T> valueTypeRef) throws JsonProcessingException, UnsupportedEncodingException {
        return new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(response.andReturn().getResponse().getContentAsString(),
                        valueTypeRef);
    }

    public static MockMultipartHttpServletRequestBuilder putMultipart(String fileWebTarget) {
        MockMultipartHttpServletRequestBuilder builder = multipart(fileWebTarget);
        builder.with(request -> {
            request.setMethod("PUT");
            return request;
        });
        return builder;
    }

}

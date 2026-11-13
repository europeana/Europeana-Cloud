package eu.europeana.cloud.service.mcs.utils;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EnrichUriServiceTest {


  private static final String EXTERNAL_URI = "https://test.ecloud.psnc.pl/api";
  private static final String CONTEXT_PATH = "/mcs";

  private EnrichUriService enrichUriService;
  private HttpServletRequest request;

  @BeforeEach
  void setUp() {
    enrichUriService = new EnrichUriService(EXTERNAL_URI);

    MockHttpServletRequest mockRequest = new MockHttpServletRequest();
    mockRequest.setScheme("http");
    mockRequest.setServerName("mcs-service");
    mockRequest.setServerPort(8080);
    mockRequest.setContextPath(CONTEXT_PATH);
    mockRequest.setRequestURI(CONTEXT_PATH + "/representations");

    request = mockRequest;
  }

  @Test
  void shouldEnrichRepresentation() {
    // given
    Representation representation = mock(Representation.class);

    when(representation.getCloudId()).thenReturn("cloud123");
    when(representation.getRepresentationName()).thenReturn("representation1");
    when(representation.getVersion()).thenReturn("v1");
    when(representation.getFiles()).thenReturn(null);

    // when
    enrichUriService.enrich(request, representation);

    // then
    verify(representation).setAllVersionsUri(argThat(uri ->
            isExternalUri(uri) &&
                    uri.getPath().contains("cloud123") &&
                    uri.getPath().contains("representation1")
    ));

    verify(representation).setUri(argThat(uri ->
            isExternalUri(uri) &&
                    uri.getPath().contains("cloud123") &&
                    uri.getPath().contains("representation1") &&
                    uri.getPath().contains("v1")
    ));
  }

  @Test
  void shouldNotSetRepresentationVersionUriWhenVersionIsNull() {
    // given
    Representation representation = mock(Representation.class);

    when(representation.getCloudId()).thenReturn("cloud123");
    when(representation.getRepresentationName()).thenReturn("representation1");
    when(representation.getVersion()).thenReturn(null);
    when(representation.getFiles()).thenReturn(null);

    // when
    enrichUriService.enrich(request, representation);

    // then
    verify(representation).setAllVersionsUri(any(URI.class));
    verify(representation, never()).setUri(any(URI.class));
  }

  @Test
  void shouldEnrichRepresentationFiles() {
    // given
    Representation representation = mock(Representation.class);
    File file = mock(File.class);

    when(representation.getCloudId()).thenReturn("cloud123");
    when(representation.getRepresentationName()).thenReturn("representation1");
    when(representation.getVersion()).thenReturn("v1");
    when(representation.getFiles()).thenReturn(List.of(file));

    when(file.getFileName()).thenReturn("image.jpg");

    // when
    enrichUriService.enrich(request, representation);

    // then
    verify(file).setContentUri(argThat(uri ->
            isExternalUri(uri) &&
                    uri.getPath().contains("cloud123") &&
                    uri.getPath().contains("representation1") &&
                    uri.getPath().contains("v1") &&
                    uri.getPath().contains("image.jpg")
    ));
  }

  @Test
  void shouldEnrichDataSet() {
    // given
    DataSet dataSet = mock(DataSet.class);

    when(dataSet.getProviderId()).thenReturn("provider123");
    when(dataSet.getId()).thenReturn("dataset456");

    // when
    enrichUriService.enrich(request, dataSet);

    // then
    verify(dataSet).setUri(argThat(uri ->
            isExternalUri(uri) &&
                    uri.getPath().contains("provider123") &&
                    uri.getPath().contains("dataset456")
    ));
  }

  @Test
  void shouldEnrichRepresentationRevisionResponse() {
    // given
    RepresentationRevisionResponse response =
            mock(RepresentationRevisionResponse.class);

    when(response.getCloudId()).thenReturn("cloud123");
    when(response.getRepresentationName()).thenReturn("representation1");
    when(response.getVersion()).thenReturn("v1");
    when(response.getFiles()).thenReturn(null);

    // when
    enrichUriService.enrich(request, response);

    // then
    verify(response).setRepresentationVersionUri(argThat(uri ->
            isExternalUri(uri) &&
                    uri.getPath().contains("cloud123") &&
                    uri.getPath().contains("representation1") &&
                    uri.getPath().contains("v1")
    ));
  }

  @Test
  void shouldEnrichRecordRepresentations() {
    // given
    eu.europeana.cloud.common.model.Record record = mock(Record.class);
    Representation representation1 = mock(Representation.class);
    Representation representation2 = mock(Representation.class);

    when(record.getRepresentations())
            .thenReturn(List.of(representation1, representation2));

    when(representation1.getCloudId()).thenReturn("cloud1");
    when(representation1.getRepresentationName()).thenReturn("rep1");
    when(representation1.getVersion()).thenReturn(null);
    when(representation1.getFiles()).thenReturn(null);

    when(representation2.getCloudId()).thenReturn("cloud2");
    when(representation2.getRepresentationName()).thenReturn("rep2");
    when(representation2.getVersion()).thenReturn(null);
    when(representation2.getFiles()).thenReturn(null);

    // when
    enrichUriService.enrich(request, record);

    // then
    verify(representation1).setAllVersionsUri(any(URI.class));
    verify(representation2).setAllVersionsUri(any(URI.class));
  }

  @Test
  void shouldThrowExceptionWhenExternalUriIsInvalid() {
    // given
    String invalidUri = "://invalid-uri";

    // when
    RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> new EnrichUriService(invalidUri)
    );

    // then
    assertTrue(exception.getMessage().contains(invalidUri));
    assertInstanceOf(URISyntaxException.class, exception.getCause());
  }

  private boolean isExternalUri(URI uri) {
    return uri != null
            && "https".equals(uri.getScheme())
            && "test.ecloud.psnc.pl".equals(uri.getHost())
            && uri.getPort() == -1
            && uri.getPath().startsWith("/api/");
  }

  @Test
  void testMappingPlaceholdersResolver() {
    EnrichUriService.MappingPlaceholdersResolver r = new EnrichUriService.MappingPlaceholdersResolver();

    final String CLASS_MAPPING =
            "/records/{" + CLOUD_ID + "}/representations/{" + REPRESENTATION_NAME + "}/versions/{" + VERSION + "}/files/{" + FILE_NAME
                    + ":(.+)?}";

    Properties properties = new Properties();
    properties.setProperty(CLOUD_ID, "cloud_id");
    properties.setProperty(REPRESENTATION_NAME, "representation_name");
    properties.setProperty(VERSION, "version");
    properties.setProperty(FILE_NAME, "file_name");

    String s = r.replacePlaceholders(CLASS_MAPPING, properties);
    System.err.println(s);
  }

}

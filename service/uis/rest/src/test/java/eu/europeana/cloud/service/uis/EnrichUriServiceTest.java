package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.uis.rest.EnrichUriService;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class EnrichUriServiceTest {

    private EnrichUriService enrichUriService;
    private HttpServletRequest request;
    private DataProvider provider;

    @BeforeEach
    void setUp() {
        enrichUriService = new EnrichUriService("https://external.example.com:8443/external");
        request = mock(HttpServletRequest.class);
        provider = mock(DataProvider.class);

        when(provider.getId()).thenReturn("provider123");
        when(request.getContextPath()).thenReturn("/internal");
    }

    @Test
    void shouldReplaceRequestBaseUriWithExternalUri() {
        // given
        when(request.getRequestURL())
                .thenReturn(new StringBuffer("http://internal-service:8080/internal/providers"));

        when(request.getRequestURI()).thenReturn("/internal/providers");

        // when
        enrichUriService.enrich(request, provider);

        // then
        verify(provider).setUri(URI.create(
                "https://external.example.com:8443/external/providers/provider123"
        ));
    }

    @Test
    void shouldPreserveOriginalRequestPath() {
        // given
        when(request.getRequestURL())
                .thenReturn(new StringBuffer("http://internal-service:8080/internal/api/v1/providers"));

        when(request.getRequestURI()).thenReturn("/internal/api/v1/providers");

        // when
        enrichUriService.enrich(request, provider);

        // then
        verify(provider).setUri(URI.create(
                "https://external.example.com:8443/external/api/v1/providers/provider123"
        ));
    }

    @Test
    void shouldPreserveOriginalQueryParameters() {
        // given
        when(request.getRequestURL())
                .thenReturn(new StringBuffer("http://internal-service:8080/internal"));

        when(request.getRequestURI()).thenReturn("/internal/providers");
        when(request.getQueryString()).thenReturn("active=true");

        // when
        enrichUriService.enrich(request, provider);

        // then
        verify(provider).setUri(URI.create(
                "https://external.example.com:8443/external/providers/provider123?active=true"
        ));
    }

    @Test
    void shouldAppendProviderIdAsPathSegment() {
        // given
        when(provider.getId()).thenReturn("provider/with spaces");

        when(request.getRequestURL())
                .thenReturn(new StringBuffer("http://internal-service:8080/internal/providers"));

        when(request.getRequestURI()).thenReturn("/internal/providers");

        // when
        enrichUriService.enrich(request, provider);

        // then
        verify(provider).setUri(URI.create(
                "https://external.example.com:8443/external/providers/provider/with%20spaces"
        ));
    }

    @Test
    void shouldSupportExternalUriWithoutExplicitPort() {
        // given
        enrichUriService = new EnrichUriService("https://external.example.com/external");

        when(request.getRequestURL())
                .thenReturn(new StringBuffer("http://internal-service:8080/internal/providers"));

        when(request.getRequestURI()).thenReturn("/internal/providers");

        // when
        enrichUriService.enrich(request, provider);

        // then
        verify(provider).setUri(URI.create(
                "https://external.example.com/external/providers/provider123"
        ));
    }

    @Test
    void shouldThrowExceptionWhenExternalUriIsInvalid() {
        // given
        String invalidUri = "://invalid-uri";

        // when & then
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> new EnrichUriService(invalidUri)
        );

        assertTrue(exception.getMessage().contains(invalidUri));
        assertInstanceOf(
                java.net.URISyntaxException.class,
                exception.getCause()
        );
    }
}

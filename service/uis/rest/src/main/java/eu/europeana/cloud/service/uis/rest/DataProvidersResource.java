package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.RestInterfaceConstants;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Resource for DataProviders.
 */
@RestController
@RequestMapping(RestInterfaceConstants.DATA_PROVIDERS)
public class DataProvidersResource {

  private static final int NUMBER_OF_ELEMENTS_ON_PAGE = 100;
  private final DataProviderService providerService;

  /**
   * Constructor used for injection
   *
   * @param providerService service for providers
   */
  public DataProvidersResource(DataProviderService providerService) {
    this.providerService = providerService;
  }

  /**
   * Lists all providers stored in eCloud. Result is returned in slices.
   *
   * @param from data provider identifier from which returned slice of results will be generated. If not provided then result list
   * will contain data providers from the first one.
   * @return one slice of result containing eCloud data providers.
   */
  @GetMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
  public ResultSlice<DataProvider> getProviders(
      @RequestParam(value = "from", required = false) String from) {
    return providerService.getProviders(from, NUMBER_OF_ELEMENTS_ON_PAGE);
  }

  /**
   * Creates a new data provider. Response contains uri to created resource in as content location.
   *
   * @param dataProviderProperties <strong>REQUIRED</strong> data provider properties.
   * @param providerId <strong>REQUIRED</strong> data provider identifier for newly created provider
   * @return URI to created resource in content location
   * @throws ProviderAlreadyExistsException provider already exists.
   * @statuscode 201 new provider has been created.
   * @statuscode 400 request body cannot be is empty
   */
  @PostMapping(
      produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE},
      consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE}
  )
  @PreAuthorize("isAuthenticated()")
  public ResponseEntity<String> createProvider(HttpServletRequest servletRequest,
      @RequestBody DataProviderProperties dataProviderProperties,
      @RequestParam String providerId) throws ProviderAlreadyExistsException {
    DataProvider provider = providerService.createProvider(providerId,
        dataProviderProperties);
    EnrichUriUtil.enrich(servletRequest, provider);
    return ResponseEntity.created(provider.getUri()).build();
  }
}

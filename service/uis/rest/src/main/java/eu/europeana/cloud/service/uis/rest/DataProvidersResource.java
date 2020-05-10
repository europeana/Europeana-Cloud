package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.uis.ACLServiceWrapper;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;

/**
 * Resource for DataProviders.
 *
 */
@RestController
@RequestMapping("/data-providers")
public class DataProvidersResource {

    private DataProviderService providerService;
	private ACLServiceWrapper aclWrapper;

	private static final int NUMBER_OF_ELEMENTS_ON_PAGE = 100;
	private final String DATA_PROVIDER_CLASS_NAME = DataProvider.class.getName();

	public DataProvidersResource(
			DataProviderService providerService,
			ACLServiceWrapper aclWrapper) {
		this.providerService = providerService;
		this.aclWrapper = aclWrapper;
	}
    /**
     * Lists all providers stored in eCloud. Result is returned in slices.
     *
	 * @summary All providers list
	 *
     * @param startFrom
     *            data provider identifier from which returned slice of results will be generated.
	 *            If not provided then result list will contain data providers from the first one.
	 *
     * @return one slice of result containing eCloud data providers.
     */
    @GetMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
	public ResultSlice<DataProvider> getProviders(
            @RequestParam(value = UISParamConstants.Q_FROM,required = false) String startFrom) {
		return providerService.getProviders(startFrom, NUMBER_OF_ELEMENTS_ON_PAGE);
	}

    /**
     * Creates a new data provider. Response contains uri to created resource in
     * as content location.
     *
	 * @summary Data provider creation
	 *
     * @param dataProviderProperties
     *            <strong>REQUIRED</strong> data provider properties.
     * @param providerId
     *            <strong>REQUIRED</strong> data provider identifier for newly created provider
     * @return URI to created resource in content location
     * @throws ProviderAlreadyExistsException
     *             provider already exists.
     * @statuscode 201 new provider has been created.
	 * @statuscode 400 request body cannot be is empty
     */
	@PostMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE}, consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
	@PreAuthorize("isAuthenticated()")
    public ResponseEntity<String> createProvider(HttpServletRequest servletRequest,
                                                 @RequestBody DataProviderProperties dataProviderProperties,
                                                 @RequestParam(UISParamConstants.Q_PROVIDER) String providerId)
			throws ProviderAlreadyExistsException, URISyntaxException {
	DataProvider provider = providerService.createProvider(providerId,
		dataProviderProperties);
	EnrichUriUtil.enrich(servletRequest, provider);

	// provider created => let's assign permissions to the owner
	String creatorName = SpringUserUtils.getUsername();
	if (creatorName != null) {
	    ObjectIdentity providerIdentity = new ObjectIdentityImpl(
		    DATA_PROVIDER_CLASS_NAME, providerId);

	    MutableAcl providerAcl = aclWrapper.getAcl(creatorName, providerIdentity);
	    aclWrapper.updateAcl(providerAcl);
	}

	return ResponseEntity.created(provider.getUri()).build();
    }
}

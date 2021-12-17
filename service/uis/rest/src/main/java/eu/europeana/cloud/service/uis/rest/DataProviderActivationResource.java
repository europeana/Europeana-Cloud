package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.uis.DataProviderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

@RestController
@RequestMapping("/data-providers/{" + P_PROVIDER + "}/active")
public class DataProviderActivationResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataProviderActivationResource.class);

    private DataProviderService providerService;

    public DataProviderActivationResource(DataProviderService providerService){
        this.providerService = providerService;
    }

    /**
     * Activates data provider (sets flag 'active' to true)
     * 
     * <br/><br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
     *      <strong>Required permissions:</strong>
     *      <ul>
     *          <li>Admin role</li>
     *      </ul>
     * </div>    
     * 
     * @summary Data-provider activation 
     * @param dataProviderId <strong>REQUIRED</strong> identifier of data-provider which is about to be activated 
     * @return Empty response with http status code indicating whether the operation was successful or not
     * @author 
     * @throws ProviderDoesNotExistException Supplied Data-provider does not exist
     * 
     */
    @PutMapping
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public ResponseEntity<Void> activateDataProvider(@PathVariable(P_PROVIDER) String dataProviderId) throws ProviderDoesNotExistException {
        LOGGER.info("Activating data provider: {}", dataProviderId);

        DataProvider dataProvider = providerService.getProvider(dataProviderId);
        dataProvider.setActive(true);
        providerService.updateProvider(dataProvider);

        return ResponseEntity.ok().build();
    }

    /**
     * Deactivates data provider (sets flag 'active' to false)
     * 
     * <br/><br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
     *      <strong>Required permissions:</strong>
     *      <ul>
     *          <li>Admin role</li>
     *      </ul>
     * </div>
     * 
     * @summary Data-provider deactivation
     * @param dataProviderId <strong>REQUIRED</strong> identifier of data-provider which is about to be activated    
     * @return Empty response with http status code indicating whether the operation was successful or not
     * @author
     * @throws ProviderDoesNotExistException Supplied Data-provider does not exist 
     */
    @DeleteMapping
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public ResponseEntity<Void> deactivateDataProvider(@PathVariable(P_PROVIDER) String dataProviderId) throws ProviderDoesNotExistException {
        LOGGER.info("Deactivating data provider: {}", dataProviderId);

        DataProvider dataProvider = providerService.getProvider(dataProviderId);
        dataProvider.setActive(false);
        providerService.updateProvider(dataProvider);

        return ResponseEntity.ok().build();
    }

}

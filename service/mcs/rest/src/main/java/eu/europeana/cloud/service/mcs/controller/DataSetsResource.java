package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.DATA_SETS_RESOURCE;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import javax.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Resource to get and create data set.
 */
@RestController
@RequestMapping(DATA_SETS_RESOURCE)
public class DataSetsResource {

  private static final String DATASET_CLASS_NAME = DataSet.class.getName();

  private final DataSetService dataSetService;
  private final MutableAclService mutableAclService;

  @Value("${numberOfElementsOnPage}")
  private int numberOfElementsOnPage;

  public DataSetsResource(DataSetService dataSetService, MutableAclService mutableAclService) {
    this.dataSetService = dataSetService;
    this.mutableAclService = mutableAclService;
  }

  /**
   * Returns all data sets for a provider. Result is returned in slices.
   *
   * @param providerId provider id for which returned data sets will belong to (required)
   * @param startFrom reference to next slice of result. If not provided, first slice of result will be returned.
   * @return slice of data sets for a given provider.
   * @summary get provider's data sets
   */
  @GetMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  public ResultSlice<DataSet> getDataSets(
      @PathVariable String providerId,
      @RequestParam(required = false) String startFrom) {

    return dataSetService.getDataSets(providerId, startFrom, numberOfElementsOnPage);
  }

  /**
   * Creates a new data set.
   *
   * <strong>User permissions required.</strong>
   *
   * @param providerId the provider for the created data set
   * @param dataSetId identifier of the data set (required).
   * @param description description of the data set.
   * @return URI to the newly created data set in content-location.
   * @throws ProviderNotExistsException data provider does not exist.
   * @throws eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException data set with this id already exists
   * @statuscode 201 object has been created.
   */
  @PreAuthorize("isAuthenticated()")
  @PostMapping
  public ResponseEntity<Void> createDataSet(
      HttpServletRequest httpServletRequest,
      @PathVariable String providerId,
      @RequestParam String dataSetId,
      @RequestParam(required = false) String description) throws ProviderNotExistsException, DataSetAlreadyExistsException {

    DataSet dataSet = dataSetService.createDataSet(providerId, dataSetId, description);
    EnrichUriUtil.enrich(httpServletRequest, dataSet);

    String creatorName = SpringUserUtils.getUsername();
    if (creatorName != null) {
      ObjectIdentity dataSetIdentity = new ObjectIdentityImpl(DATASET_CLASS_NAME, dataSetId + "/" + providerId);

      MutableAcl datasetAcl = mutableAclService.createAcl(dataSetIdentity);

      datasetAcl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
      datasetAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
      datasetAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
      datasetAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName), true);

      mutableAclService.updateAcl(datasetAcl);
    }
    return ResponseEntity.created(dataSet.getUri()).build();
  }
}

package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.SIMPLIFIED_RECORDS_RESOURCE;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Gives (read) access to record stored in ecloud in simplified (friendly) way.
 */
@RestController
@RequestMapping(SIMPLIFIED_RECORDS_RESOURCE)
public class SimplifiedRecordsResource {

  private final RecordService recordService;
  private final UISClientHandler uisHandler;

  public SimplifiedRecordsResource(RecordService recordService, UISClientHandler uisHandler) {
    this.recordService = recordService;
    this.uisHandler = uisHandler;
  }

  /**
   * Returns record with all representations
   *
   * @param httpServletRequest
   * @param providerId providerId
   * @param localId localId
   * @return record with all representations
   * @throws CloudException
   * @throws RecordNotExistsException
   * @summary Get record using simplified url
   */
  @GetMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
  public Record getRecord(
      HttpServletRequest httpServletRequest,
      @PathVariable String providerId,
      @PathVariable String localId) throws RecordNotExistsException, ProviderNotExistsException {

    final String cloudId = findCloudIdFor(providerId, localId);

    Record record = recordService.getRecord(cloudId);
    prepare(httpServletRequest, record);
    return record;
  }

  private String findCloudIdFor(String providerId, String localId) throws ProviderNotExistsException, RecordNotExistsException {
    CloudId foundCloudId = uisHandler.getCloudIdFromProviderAndLocalId(providerId, localId);
    return foundCloudId.getId();
  }

  private void prepare(HttpServletRequest httpServletRequest, Record record) {
    EnrichUriUtil.enrich(httpServletRequest, record);
    for (Representation representation : record.getRepresentations()) {
      representation.setCloudId(null);
    }
  }
}

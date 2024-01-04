package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.TopologyGeneralException;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Optional;

/**
 * Class finds Europeana id among different ids mapped to one given cloud id, stored in UIS. Class is needed cause UIS is rather
 * general service and it does not have information about local_id type, it stores all the ids on the list, which can be get. So
 * after getting this list, it must be distinguished which id is the Europeana id.
 */
public class EuropeanaIdFinder {

  private final UISClient uisClient;

  private final HarvestedRecordsDAO harvestedRecordsDAO;

  public EuropeanaIdFinder(UISClient uisClient, HarvestedRecordsDAO harvestedRecordsDAO) {
    this.uisClient = uisClient;
    this.harvestedRecordsDAO = harvestedRecordsDAO;
  }

  public String findForFileUrl(String metisDatasetId, String fileUrl) throws MalformedURLException, CloudException {
    return findForCloudId(metisDatasetId, extractCloudIdFromUrl(fileUrl));
  }

  public String findForCloudId(String metisDatasetId, String cloudId) throws CloudException {
    List<String> localIds = findLocalIdsInUIS(cloudId);
    return theOneAndOnlyOneIn(localIds)
        .orElseGet(() -> theOneAndOnlyOneIn(listOfIdsPrefixedByMetisDatasetId(metisDatasetId, localIds))
            .orElseGet(() -> theOneAndOnlyOneIn(listOfIdsWhichEuropeanaPostfixIsPartOfRestOfIds(metisDatasetId, localIds))
                .orElseGet(() -> theOneAndOnlyOneIn(listOfIdsThatExistInHarvestedRecordsTable(metisDatasetId, localIds))
                    .orElseThrow(() -> new TopologyGeneralException(
                        "Could not resolve unambiguous EuropeanaId for cloudId: " + cloudId)))));
  }

  private Optional<String> theOneAndOnlyOneIn(List<String> ids) {
    return ids.size() == 1 ? Optional.of(ids.get(0)) : Optional.empty();
  }

  private List<String> listOfIdsPrefixedByMetisDatasetId(String metisDatasetId, List<String> localIds) {
    return localIds.stream().filter(id -> isEuropeanaId(metisDatasetId, id)).toList();
  }

  private List<String> listOfIdsWhichEuropeanaPostfixIsPartOfRestOfIds(String metisDatasetId, List<String> localIds) {
    List<String> ids = listOfIdsPrefixedByMetisDatasetId(metisDatasetId, localIds);
    return ids.stream().filter(id -> ids.stream().allMatch(
        otherId -> otherId.contains(europeanaPostfix(id, metisDatasetId)))).toList();
  }

  private List<String> listOfIdsThatExistInHarvestedRecordsTable(String metisDatasetId, List<String> localIds) {
    return localIds.stream()
                   .filter(id -> existsInHarvestedRecordsTable(id, metisDatasetId)).toList();
  }

  private boolean isEuropeanaId(String metisDatasetId, String id) {
    return id.startsWith(europeanaPrefix(metisDatasetId));
  }

  private String europeanaPostfix(String europeanaId, String metisDatasetId) {
    return europeanaId.substring(europeanaPrefix(metisDatasetId).length());
  }

  private String europeanaPrefix(String metisDatasetId) {
    return "/" + metisDatasetId + "/";
  }

  private List<String> findLocalIdsInUIS(String cloudIdentifier) throws CloudException {
    return RetryableMethodExecutor.executeOnRest(
                                      "Could not get record id for cloud id: " + cloudIdentifier + " from UIS ",
                                      () -> uisClient.getRecordId(cloudIdentifier)).getResults().stream()
                                  .map(CloudId::getLocalId).map(LocalId::getRecordId)
                                  .toList();
  }

  private boolean existsInHarvestedRecordsTable(String cloudId, String metisDatasetId) {
    return harvestedRecordsDAO.findRecord(metisDatasetId, cloudId).isPresent();
  }

  private String extractCloudIdFromUrl(String fileUrl) throws MalformedURLException {
    return new UrlParser(fileUrl).getPart(UrlPart.RECORDS);
  }

}

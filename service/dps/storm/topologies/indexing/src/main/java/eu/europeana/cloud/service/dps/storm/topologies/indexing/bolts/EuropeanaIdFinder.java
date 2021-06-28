package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.storm.TopologyGeneralException;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;

import java.net.MalformedURLException;
import java.util.List;
import java.util.stream.Collectors;

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
        List<String> cloudIdList = findLocalIdsInUIS(cloudId);

        if (cloudIdList.size() == 1) {
            return cloudIdList.get(0);
        }

        List<String> idsMetisIdPrefix = cloudIdList.stream().filter(id -> isEuropeanaId(metisDatasetId, id)).collect(Collectors.toList());
        if (idsMetisIdPrefix.size() == 1) {
            return idsMetisIdPrefix.get(0);
        }

        if (idsMetisIdPrefix.size() > 1) {
            List<String> idsWhichLocalPartIsContainedInRestOfIds = idsMetisIdPrefix.stream().filter(
                    id -> idsMetisIdPrefix.stream().allMatch(
                            otherId -> otherId.contains(localIdPart(id, metisDatasetId)))).collect(Collectors.toList());

            if (idsWhichLocalPartIsContainedInRestOfIds.size() == 1) {
                return idsWhichLocalPartIsContainedInRestOfIds.get(0);
            }
        }

        List<String> idsThatExistsInHarvestedRecordsTable = cloudIdList.stream()
                .filter(id -> existsInHarvestedRecordsTable(id, metisDatasetId)).collect(Collectors.toList());

        if (idsThatExistsInHarvestedRecordsTable.size() == 1) {
            return idsThatExistsInHarvestedRecordsTable.get(0);
        }

        throw new TopologyGeneralException("Could not resolve unambiguous EuropeanaId for cloudId: " + cloudId);
    }

    private boolean isEuropeanaId(String metisDatasetId, String id) {
        return id.startsWith(europeanaPrefix(metisDatasetId));
    }

    private String localIdPart(String europeanaId, String metisDatasetId) {
        return europeanaId.substring(europeanaPrefix(metisDatasetId).length());
    }

    private String europeanaPrefix(String metisDatasetId) {
        return "/" + metisDatasetId + "/";
    }

    private List<String> findLocalIdsInUIS(String cloudIdentifier) throws CloudException {
        return uisClient.getRecordId(cloudIdentifier).getResults().stream().map(CloudId::getLocalId).map(LocalId::getRecordId).collect(Collectors.toList());
    }

    private boolean existsInHarvestedRecordsTable(String cloudId, String metisDatasetId) {
        return harvestedRecordsDAO.findRecord(metisDatasetId, cloudId).isPresent();
    }

    private String extractCloudIdFromUrl(String fileUrl) throws MalformedURLException {
        return new UrlParser(fileUrl).getPart(UrlPart.RECORDS);
    }

}

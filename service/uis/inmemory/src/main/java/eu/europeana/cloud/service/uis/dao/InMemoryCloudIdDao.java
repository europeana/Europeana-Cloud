package eu.europeana.cloud.service.uis.dao;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;

/**
 * In Memory Cloud Id implementation
 *
 * @author Yorgos Mamakis (Yorgos.Mamakis@ europeana.eu)
 * @since Dec 20, 2013
 */
public class InMemoryCloudIdDao {

    private static List<InMemoryCloudObject> cloudIds = new ArrayList<>();

    public List<CloudId> searchById(boolean deleted, String... args) throws DatabaseConnectionException {
        List<CloudId> retCloudIds = new ArrayList<>();

        for (InMemoryCloudObject obj : cloudIds) {
            if (obj.getCloudId().contentEquals(args[0]) && obj.isDeleted() == deleted) {
                CloudId cId = new CloudId();
                cId.setId(obj.getCloudId());
                LocalId lId = new LocalId();
                lId.setProviderId(obj.getProviderId());
                lId.setRecordId(obj.getRecordId());
                cId.setLocalId(lId);
                retCloudIds.add(cId);
            }
        }
        return retCloudIds;
    }

    public List<CloudId> searchActive(String... args) throws DatabaseConnectionException {
        return searchById(false, args);
    }

    /**
     * Method that searches both active and inactive identifiers
     *
     * @param cloudId The global Identifier to search on
     * @return A List of Cloud Identifiers
     * @throws DatabaseConnectionException
     */
    public List<CloudId> searchAll(String cloudId) throws DatabaseConnectionException {
        List<CloudId> cIds = new ArrayList<>();
        for (InMemoryCloudObject obj : cloudIds) {
            if (obj.getCloudId().contentEquals(cloudId)) {
                CloudId cId = new CloudId();
                cId.setId(cloudId);
                LocalId lId = new LocalId();
                lId.setProviderId(obj.getProviderId());
                lId.setRecordId(obj.getRecordId());
                cId.setLocalId(lId);
                cIds.add(cId);
            }
        }
        return cIds;
    }

    public List<CloudId> insert(String... args) throws DatabaseConnectionException {
        InMemoryCloudObject obj = new InMemoryCloudObject();
        obj.setCloudId(args[0]);
        obj.setProviderId(args[1]);
        obj.setRecordId(args[2]);
        obj.setDeleted(false);
        cloudIds.add(obj);
        final CloudId cId = new CloudId();
        cId.setId(args[0]);
        LocalId lId = new LocalId();
        lId.setProviderId(args[1]);
        lId.setRecordId(args[2]);
        cId.setLocalId(lId);
        return ImmutableList.of(cId);
    }

    public void delete(String... args) throws DatabaseConnectionException {
        for (InMemoryCloudObject obj : cloudIds) {
            if (obj.getCloudId().contentEquals(args[0]) && obj.getProviderId().contentEquals(args[1]) && obj.getRecordId().contentEquals(args[2])) {
                obj.setDeleted(true);
            }
        }
    }

    public void update(String... args) throws DatabaseConnectionException {
        throw new UnsupportedOperationException();
    }

    public String getHost() {
        return "";
    }

    public String getKeyspace() {
        return "";
    }

    public String getPort() {
        return "";
    }

    /**
     * Method that empties the Cloud Id cache
     */
    public void reset() {
        cloudIds = new ArrayList<>();
    }
}

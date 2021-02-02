package eu.europeana.cloud.persisted;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetIterator;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.utils.CloudIdReader;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ProviderRemover {
    static final Logger LOGGER = Logger.getLogger(ProviderRemover.class);

    private static final int FETCH_LIMIT = 1000;
    private final String url;
    private final String username;
    private final String password;
    private final boolean testMode;

    private static final int RECORD_CONNECT_TIMEOUT = 5*60*1000;
    private static final int RECORD_READ_TIMEOUT = 10*60*1000;

    public ProviderRemover(String url, String username, String password, boolean testMode) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.testMode = testMode;
    }

    public void removeAllRecords(List<String> providers) throws CloudException {
        for(String providerId: providers) {
            removeRecordsForProvider(providerId);
        }
    }

    public void removeRecordsFromFile(String filename) throws IOException {
        UISClient uisClient = new UISClient(url, username, password);
        CloudIdReader reader = new CloudIdReader(filename);

        List<String> cloudIds;
        int counter = 0;
        do {
            cloudIds = reader.getNextCloudId(FETCH_LIMIT);
            counter = removeRecords(uisClient, cloudIds, counter);
        } while(cloudIds.size() == FETCH_LIMIT);
        reader.close();
    }

    public void removeRecordsForProvider(String providerId) throws CloudException {
        UISClient uisClient = new UISClient(url, username, password);
        String startRecordId = "";
        ResultSlice<CloudId> records = null;
        int counter = 0;

        do {
            try {
                records = uisClient.getCloudIdsByProviderWithPagination(providerId, startRecordId, FETCH_LIMIT);
            }catch(CloudException ce) {
                if(ce.getMessage().equals("PROVIDER_DOES_NOT_EXIST")) {
                    LOGGER.info(String.format("Provider '%s' doen't exists", providerId) );
                } else {
                    throw ce;  //re-throw others/unexpected CloudException
                }
            }

            if (records != null && records.getResults() != null && !records.getResults().isEmpty()) {
                startRecordId = records.getNextSlice();
                counter = removeRecords(uisClient, records.getResults(), counter);
            } else {
                startRecordId = null;
            }
        } while (startRecordId != null);

        LOGGER.info(String.format("Total processed records for provider '%s' : %d", providerId, counter));
    }

    private int removeRecords(UISClient uisClient, List<?> records, int counter) {
        RecordServiceClient recordServiceClient =
                new RecordServiceClient(url, null, username, password, RECORD_CONNECT_TIMEOUT, RECORD_READ_TIMEOUT);
        Set<String> removedRecords = new HashSet<>();

        for (Object cloudObject : records) {
            String id;
            if(cloudObject instanceof CloudId) {
                id = ((CloudId) cloudObject).getId();
            } else {
                id = (String)cloudObject;
            }

            if(id == null) {
                break;
            }

            counter++;
            LOGGER.info(String.format("[%d] Remove record '%s'...", counter, id));
            if ( !testMode && !removedRecords.contains(id) ) {
                try {
                    recordServiceClient.deleteRecord(id);
                    uisClient.deleteCloudId(id);

                    LOGGER.info(String.format("Record with cloudId = '%s' removed", id));
                    removedRecords.add(id);
                } catch(MCSException | CloudException | DriverException serviceException) {
                    LOGGER.error(String.format("Error while removing record '%s' : %s", id, serviceException.getMessage()));
                }
            }
        }
        return counter;
    }

    public void removeAllDatasets(List<String> providers) {
        for(String providerId: providers) {
            removeDatasetsForProvider(providerId);
        }
    }

    public void removeDatasetsForProvider(String providerId) {
        int counter = 0;

        DataSetServiceClient dataSetServiceClient = new DataSetServiceClient(url, username, password);
        DataSetIterator dataSetIterator = dataSetServiceClient.getDataSetIteratorForProvider(providerId);

        while (dataSetIterator.hasNext()) {
            DataSet dataSet = dataSetIterator.next();
            LOGGER.info(String.format("[%d] Remove dataset '%s'...", counter, dataSet.getId()));
            if(!testMode) {
                try {
                    dataSetServiceClient.deleteDataSet(providerId, dataSet.getId());
                    LOGGER.info(String.format("Dataset '%s' removed", dataSet.getId()));
                } catch(MCSException mcsException) {
                    LOGGER.error(String.format("Error while removing dataset '%s' : %s", dataSet.getId(), mcsException.getMessage()));
                }
            }
            counter++;
        }

        LOGGER.info(String.format("Total processed datasets for provider '%s' : %d", providerId, counter));
    }
}

package eu.europeana.cloud.persisted;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetIterator;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.log4j.Logger;

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

    public ProviderRemover(String url, String username, String password, boolean testMode) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.testMode = testMode;
    }

    public void removeAllRecords(List<String> providers) throws CloudException, MCSException {
        for(String providerId: providers) {
            removeRecordsForProvider(providerId);
        }
    }

    public void removeRecordsForProvider(String providerId) throws CloudException, MCSException {
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
                counter += removeRecords(records.getResults());
            } else {
                startRecordId = null;
            }
        } while (startRecordId != null);

        LOGGER.info(String.format("Total processed records for provider '%s' : %d", providerId, counter));
    }

    private int removeRecords(List<CloudId> records) throws MCSException {
        RecordServiceClient recordServiceClient = new RecordServiceClient(url, username, password);
        Set<String> removedRecords = new HashSet<>();

        int counter = 0;
        for (CloudId cloudId : records) {
            counter++;
            LOGGER.info(String.format("Remove record '%s'", cloudId.getId()));
            if ( !testMode && !removedRecords.contains(cloudId.getId()) ) {
                try {
                    recordServiceClient.deleteRecord(cloudId.getId());
                    removedRecords.add(cloudId.getId());
                } catch(MCSException mcsException) {
                    LOGGER.error(String.format("Error while removing record '%s' : %s", cloudId.getId(), mcsException.getMessage()));
                }
            }
        }
        return counter;
    }

    public void removeAllDatasets(List<String> providers) throws MCSException {
        for(String providerId: providers) {
            removeDatasetsForProvider(providerId);
        }
    }

    public void removeDatasetsForProvider(String providerId) throws MCSException {
        int counter = 0;

        DataSetServiceClient dataSetServiceClient = new DataSetServiceClient(url, username, password);
        DataSetIterator dataSetIterator = dataSetServiceClient.getDataSetIteratorForProvider(providerId);

        while (dataSetIterator.hasNext()) {
            DataSet dataSet = dataSetIterator.next();
            LOGGER.info(String.format("Remove dataset '%s'", dataSet.getId()));
            if(!testMode) {
                try {
                    dataSetServiceClient.deleteDataSet(providerId, dataSet.getId());
                } catch(MCSException mcsException) {
                    LOGGER.error(String.format("Error while removing dataset '%s' : %s", dataSet.getId(), mcsException.getMessage()));
                }
            }
            counter++;
        }

        LOGGER.info(String.format("Total processed datasets for provider '%s' : %d", providerId, counter));
    }
}

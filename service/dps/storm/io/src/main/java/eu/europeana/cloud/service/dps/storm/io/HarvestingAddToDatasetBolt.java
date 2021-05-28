package eu.europeana.cloud.service.dps.storm.io;

public class HarvestingAddToDatasetBolt extends AddResultToDataSetBolt{
    public HarvestingAddToDatasetBolt(String ecloudMcsAddress) {
        super(ecloudMcsAddress);
    }

    @Override
    protected boolean shouldAddDeletedRecordToDataset() {
        return true;
    }

}

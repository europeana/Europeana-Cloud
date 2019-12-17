package eu.europeana.cloud.service.dps.converters;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.Harvest;
import eu.europeana.cloud.service.dps.InputDataType;

import java.util.ArrayList;
import java.util.List;

public class DpsTaskToHarvestConverter implements GenericOneToManyConverter<DpsTask, Harvest> {

    @Override
    public List<Harvest> from(DpsTask dpsTask) {
        List<Harvest> harvestsToByExecuted = new ArrayList<>();
        for (String repoUrl : dpsTask.getDataEntry(InputDataType.REPOSITORY_URLS)) {
            for (String schema : dpsTask.getHarvestingDetails().getSchemas()) {
                if (dpsTask.getHarvestingDetails().getSets() != null && dpsTask.getHarvestingDetails().getSets().size() > 0) {
                    for (String set : dpsTask.getHarvestingDetails().getSets()) {
                        Harvest h = Harvest.builder()
                                .url(repoUrl)
                                .from(dpsTask.getHarvestingDetails().getDateFrom())
                                .until(dpsTask.getHarvestingDetails().getDateUntil())
                                .setSpec(set)
                                .metadataPrefix(schema)
                                .build();
                        harvestsToByExecuted.add(h);
                    }
                } else {
                    Harvest h = Harvest.builder()
                            .url(repoUrl)
                            .from(dpsTask.getHarvestingDetails().getDateFrom())
                            .until(dpsTask.getHarvestingDetails().getDateUntil())
                            .metadataPrefix(schema)
                            .build();
                    harvestsToByExecuted.add(h);
                }
            }
        }
        return harvestsToByExecuted;
    }

    @Override
    public DpsTask to(Harvest harvest) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}

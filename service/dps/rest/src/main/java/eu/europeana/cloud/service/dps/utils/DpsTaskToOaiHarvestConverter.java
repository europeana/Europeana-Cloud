package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.converters.GenericOneToManyConverter;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class DpsTaskToOaiHarvestConverter implements GenericOneToManyConverter<DpsTask, OaiHarvest> {

    @Override
    public List<OaiHarvest> from(DpsTask dpsTask) {
        List<OaiHarvest> harvestsToByExecuted = new ArrayList<>();
        for (String repoUrl : dpsTask.getDataEntry(InputDataType.REPOSITORY_URLS)) {
            for (String schema : dpsTask.getHarvestingDetails().getSchemas()) {
                if (dpsTask.getHarvestingDetails().getSets() != null && !dpsTask.getHarvestingDetails().getSets().isEmpty()) {
                    for (String set : dpsTask.getHarvestingDetails().getSets()) {
                        OaiHarvest h = new OaiHarvest(
                                repoUrl,
                                schema,
                                set,
                                Optional.ofNullable(dpsTask.getHarvestingDetails().getDateFrom()).map(Date::toInstant).orElse(null),
                                Optional.ofNullable(dpsTask.getHarvestingDetails().getDateUntil()).map(Date::toInstant).orElse(null));
                        harvestsToByExecuted.add(h);
                    }
                } else {
                    OaiHarvest h = new OaiHarvest(
                            repoUrl,
                            schema,
                            null,
                            Optional.ofNullable(dpsTask.getHarvestingDetails().getDateFrom()).map(Date::toInstant).orElse(null),
                            Optional.ofNullable(dpsTask.getHarvestingDetails().getDateUntil()).map(Date::toInstant).orElse(null));
                    harvestsToByExecuted.add(h);
                }
            }
        }
        return harvestsToByExecuted;
    }

    @Override
    public DpsTask to(OaiHarvest oaiHarvest) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}

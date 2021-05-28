package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.converters.GenericOneToOneConverter;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;

import java.util.Date;
import java.util.Optional;

public class DpsTaskToOaiHarvestConverter implements GenericOneToOneConverter<DpsTask, OaiHarvest> {

    @Override
    public OaiHarvest from(DpsTask dpsTask) {
        return new OaiHarvest(
                dpsTask.getDataEntry(InputDataType.REPOSITORY_URLS).get(0),
                dpsTask.getHarvestingDetails().getSchema(),
                dpsTask.getHarvestingDetails().getSet(),
                Optional.ofNullable(dpsTask.getHarvestingDetails().getDateFrom()).map(Date::toInstant).orElse(null),
                Optional.ofNullable(dpsTask.getHarvestingDetails().getDateUntil()).map(Date::toInstant).orElse(null));
    }

    @Override
    public DpsTask to(OaiHarvest oaiHarvest) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}

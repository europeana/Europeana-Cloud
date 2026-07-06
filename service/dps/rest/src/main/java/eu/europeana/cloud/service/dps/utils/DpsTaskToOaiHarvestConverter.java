package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.internal.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.converters.GenericOneToOneConverter;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import java.util.Date;
import java.util.Optional;

public class DpsTaskToOaiHarvestConverter implements GenericOneToOneConverter<DpsTask, OaiHarvest> {

  @Override
  public OaiHarvest from(DpsTask dpsTask) {
    OAIPMHHarvestingDetails harvestingDetails = (OAIPMHHarvestingDetails) dpsTask.getInput();
    return new OaiHarvest(
        harvestingDetails.getRepositoryUrl(),
        harvestingDetails.getSchema(),
        harvestingDetails.getSet(),
        Optional.ofNullable(harvestingDetails.getDateFrom()).map(Date::toInstant).orElse(null),
        Optional.ofNullable(harvestingDetails.getDateUntil()).map(Date::toInstant).orElse(null));
  }

  @Override
  public DpsTask to(OaiHarvest oaiHarvest) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}

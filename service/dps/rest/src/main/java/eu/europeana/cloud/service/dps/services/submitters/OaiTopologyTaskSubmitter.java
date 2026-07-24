package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.ExpectedCounters;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.utils.DpsTaskToOaiHarvestConverter;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import eu.europeana.metis.harvesting.HarvesterRuntimeException;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import org.springframework.stereotype.Service;

@Service
public class OaiTopologyTaskSubmitter implements TaskSubmitter {


  private final HarvestsExecutor harvestsExecutor;

  public OaiTopologyTaskSubmitter(HarvestsExecutor harvestsExecutor) {
    this.harvestsExecutor = harvestsExecutor;
  }

  @Override
  public ExpectedCounters submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException, TaskDroppedException {
    OaiHarvest harvestToByExecuted = new DpsTaskToOaiHarvestConverter().from(parameters.getTask());
    try {
      int count=harvestsExecutor.execute(harvestToByExecuted, parameters);
      return new ExpectedCounters(count,0);
    } catch (HarvesterRuntimeException e) {
      throw new TaskSubmissionException("Unable to submit task properly", e);
    }
  }

}

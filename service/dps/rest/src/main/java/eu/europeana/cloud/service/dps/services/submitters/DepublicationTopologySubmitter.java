package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.service.dps.depublish.DepublicationService;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DepublicationTopologySubmitter implements TaskSubmitter {

    @Autowired
    public DepublicationService depublicationService;


    @Override
    public void submitTask(SubmitTaskParameters parameters) {
        depublicationService.depublish(parameters);
    }
}

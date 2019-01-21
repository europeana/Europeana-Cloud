package eu.europeana.cloud.service.dps.storm.topologies.dummy.bolt;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DummyBolt extends AbstractDpsBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(DummyBolt.class);


  @Override
  public void execute(StormTaskTuple stormTaskTuple) {


  }

  @Override
  public void prepare() {
  }
}
package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.schemas;


import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;

/**
 * Created by Tarek on 7/5/2017.
 */
public abstract class SchemasSplitter {
    protected Splitter splitter;

    SchemasSplitter(Splitter splitter) {
        this.splitter = splitter;

    }

    public abstract void split();


}

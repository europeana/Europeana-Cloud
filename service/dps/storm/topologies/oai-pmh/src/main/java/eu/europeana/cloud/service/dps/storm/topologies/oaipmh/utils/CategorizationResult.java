package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils;

import lombok.Builder;

/**
 * Describes result of the categorization process
 */
@Builder
public class CategorizationResult{
    private boolean toBeProcessed;

    public boolean shouldBeProcessed(){
        return toBeProcessed;
    }

    public boolean shouldBeDropped(){
        return !shouldBeProcessed();
    }
}

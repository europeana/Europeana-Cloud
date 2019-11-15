package eu.europeana.cloud.service.dps.oaipmh;

/**
 * Factory class for {@link Harvester} objects.
 */
public final class HarvesterFactory {

    private HarvesterFactory() {
        // Nothing to do: don't use this method!
    }

    /**
     * Creates a {@link Harvester}.
     *
     * @param numberOfRetries The number of times we retry a connection.
     * @param timeBetweenRetries The time we leave between two successive retries.
     * @return a harvester.
     */
    public static Harvester createHarvester(int numberOfRetries, int timeBetweenRetries) {
        return new HarvesterImpl(numberOfRetries, timeBetweenRetries);
    }
}

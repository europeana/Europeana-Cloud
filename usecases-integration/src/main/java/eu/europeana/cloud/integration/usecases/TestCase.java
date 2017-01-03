package eu.europeana.cloud.integration.usecases;

/**
 * Created by Tarek on 9/21/2016.
 */
public interface TestCase {
    void executeTestCase() throws Exception;

    void cleanUp() throws Exception;
}

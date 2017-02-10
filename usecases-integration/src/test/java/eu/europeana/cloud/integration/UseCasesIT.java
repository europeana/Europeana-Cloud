package eu.europeana.cloud.integration;

import eu.europeana.cloud.integration.usecases.ActiveRecordsTestCase;
import eu.europeana.cloud.integration.usecases.CreateDatasetFromDatasetOfAnotherProviderTestCase;
import eu.europeana.cloud.integration.usecases.UpdateDatasetTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by Tarek on 9/20/2016.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:integration-context.xml"
})
public class UseCasesIT {
    @Autowired
    private CreateDatasetFromDatasetOfAnotherProviderTestCase createDatasetFromDatasetOfAnotherProviderTestCase;
    @Autowired
    private UpdateDatasetTestCase updateDatasetTestCase;
    @Autowired
    private ActiveRecordsTestCase activeRecordsTestCase;


    @Test
    public void testCreateDatasetFromDatasetOfAnotherProviderTestCase() throws Exception {
        createDatasetFromDatasetOfAnotherProviderTestCase.executeTestCase();
        updateDatasetTestCase.executeTestCase();
        activeRecordsTestCase.executeTestCase();
    }
}

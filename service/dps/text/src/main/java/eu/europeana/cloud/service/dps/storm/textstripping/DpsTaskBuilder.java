package eu.europeana.cloud.service.dps.storm.textstripping;

import eu.europeana.cloud.service.dps.DpsTask;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author la4227 <lucas.anastasiou@open.ac.uk>
 */
public class DpsTaskBuilder {

    public static final String datasetId = "Franco_maria_performance_test_0002";
    public static final String providerId = "CORE_testing_0002";

    public static DpsTask generateDpsTask() {

        DpsTask task = new DpsTask();

        task.setTaskName("Text extraction for dataset");

        List<String> providerIdList = new ArrayList<String>();
        providerIdList.add(providerId);
        List<String> datasetIdList = new ArrayList<String>();
        datasetIdList.add(datasetId);
        task.addDataEntry("datasetId", datasetIdList);
        task.addDataEntry("providerId", providerIdList);

        task.addParameter("STORE_AS_NEW_REPRESENTATION", "TRUE");
        task.addParameter("EXTRACTION_SOFTWARE", "JPOD");

        return task;
    }
}

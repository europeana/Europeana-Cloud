package eu.europeana.cloud;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Tarek on 9/2/2016.
 */
public interface TestConstantsHelper {

    String CLOUD_ID = "CloudId";
    String REPRESENTATION_NAME = "RepresentationName";
    String VERSION = "Version";
    String FILE = "fileName";

    String EMPTY_REPRESENTATION = "EmptyRepresentation";

    String DATASET_NAME = "dataSet";
    String DATA_PROVIDER = "testDataProvider";

    String FILE_URL = "http://localhost:8080/mcs/records/"
            + CLOUD_ID + "/representations/"
            + REPRESENTATION_NAME + "/versions/"
            + VERSION + "/files/"
            + FILE;
    String FILE_URL2 = "http://localhost:8080/mcs/records/"
            + CLOUD_ID + "/representations/"
            + REPRESENTATION_NAME + "/versions/"
            + VERSION + 2 + "/files/"
            + FILE;


}

package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestUtils {

    public static Representation parseRepresentationFromUri(URI uri) {
        Representation representation = new Representation();

        String[] elements = uri.getRawPath().split("/");
        representation.setVersion(elements[elements.length - 1]);
        representation.setSchema(elements[elements.length - 3]);
        representation.setRecordId(elements[elements.length - 5]);

        return representation;
    }

    public static void assertCorrectlyCreatedRepresentation(RecordServiceClient instance, URI uri, String providerId, String cloudId, String schema)
            throws MCSException {
        assertNotNull(uri);
        Representation representationUri = parseRepresentationFromUri(uri);
        
        assertEquals(cloudId, representationUri.getRecordId());
        assertEquals(schema, representationUri.getSchema());

        //get representation and check
        Representation representation = instance.getRepresentation(cloudId, schema, representationUri.getVersion());
        assertNotNull(representation);
        assertEquals(cloudId, representation.getRecordId());
        assertEquals(schema, representation.getSchema());
        assertEquals(providerId, representation.getDataProvider());
        assertEquals(representationUri.getVersion(), representation.getVersion());
    }
}

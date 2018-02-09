package eu.europeana.cloud.service.dps.oaiTester.unit;

import com.lyncode.xoai.model.oaipmh.MetadataFormat;
import com.lyncode.xoai.model.oaipmh.Set;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.NoSetHierarchyException;
import com.lyncode.xoai.serviceprovider.model.Context;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by pwozniak on 1/31/18
 */
@RunWith(Parameterized.class)
public class ListMetadataFormatsTest extends TestSet {

    private String endpoint;

    public ListMetadataFormatsTest(String endpoint) {
        this.endpoint = endpoint;
    }

    @Test
    public void shouldListMetadata() throws NoSetHierarchyException {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Iterator<MetadataFormat> metadataFormats = serviceProvider.listMetadataFormats();
        Assert.assertNotNull(metadataFormats);
        Assert.assertTrue(metadataFormats.hasNext());
    }

}

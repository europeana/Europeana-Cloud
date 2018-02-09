package eu.europeana.cloud.service.dps.oaiTester.unit;

import com.lyncode.xoai.model.oaipmh.Identify;
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
import java.util.Date;
import java.util.Iterator;

/**
 * Created by pwozniak on 1/31/18
 */
@RunWith(Parameterized.class)
public class ListSetsTest extends TestSet {

    private String endpoint;

    public ListSetsTest(String endpoint) {
        this.endpoint = endpoint;
    }

    @Test
    public void shouldListSets() throws NoSetHierarchyException {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Iterator<Set> sets = serviceProvider.listSets();
        Assert.assertNotNull(sets);
        Assert.assertTrue(sets.hasNext());
    }
}

package eu.europeana.cloud.service.dps.oaiTester.unit;


import com.lyncode.xoai.model.oaipmh.Granularity;
import com.lyncode.xoai.model.oaipmh.Identify;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.model.Context;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Created by pwozniak on 1/30/18
 */
@RunWith(Parameterized.class)
public class IdentifyTest extends TestSet{

    private String endpoint;

    public IdentifyTest(String endpoint) {
        this.endpoint = endpoint;
    }

    @Test
    public void shouldProvideEarliestDatestamp() {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Identify identify = serviceProvider.identify();
        Date date = identify.getEarliestDatestamp();
        Assert.assertNotNull(date);
    }

    @Test
    public void shouldProvideGranularity() {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Identify identify = serviceProvider.identify();
        Granularity granularity = identify.getGranularity();
        Assert.assertNotNull("Granularity is null", granularity);
    }

    @Test
    public void granularityShouldHaveProperFormat() {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Identify identify = serviceProvider.identify();
        Granularity granularity = identify.getGranularity();
        Assert.assertNotNull(granularity);
        if (granularity.toString().equals("YYYY-MM-DD") || granularity.toString().equals("YYYY-MM-DDThh:mm:ssZ")) {
            Assert.assertTrue(true);
        } else {
            Assert.assertTrue("Granularity format is not correct", false);
        }
    }

    @Test
    public void shouldProvideAdminEmail() {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Identify identify = serviceProvider.identify();
        List<String> adminEmails = identify.getAdminEmails();
        Assert.assertNotNull("Admin Emails is null", adminEmails);
        Assert.assertTrue("Admin emails list is empty", adminEmails.size() > 0);
    }

    @Test
    public void shouldProvideProtocolVersion() {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Identify identify = serviceProvider.identify();
        String protocolVersion = identify.getProtocolVersion();
        Assert.assertNotNull("Protocol version is null", protocolVersion);
        Assert.assertTrue("Admin emails list is empty", protocolVersion.equals("2.0"));
    }

    @Test
    public void shouldProvideBaseUrl() {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Identify identify = serviceProvider.identify();
        String baseURL = identify.getBaseURL();
        Assert.assertNotNull("Protocol version is null", baseURL);
    }
}

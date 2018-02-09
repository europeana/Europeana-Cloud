package eu.europeana.cloud.service.dps.oaiTester.unit;

import com.lyncode.xoai.model.oaipmh.Header;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.BadArgumentException;
import com.lyncode.xoai.serviceprovider.model.Context;
import com.lyncode.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/**
 * Created by pwozniak on 1/30/18
 */
@RunWith(Parameterized.class)
public class ListIdentifiersTest {

    @Parameterized.Parameters
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][]{
                {"http://www.wbc.poznan.pl/dlibra/oai-pmh-repository.xml", "oai_dc"}});
    }

    private String endpoint;
    private String metadataPrefix;

    public ListIdentifiersTest(String endpoint,String metadataPrefix) {
        this.endpoint = endpoint;
        this.metadataPrefix = metadataPrefix;
    }

    @Test
    public void shouldListIdentifiers() throws BadArgumentException {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Iterator<Header> identifiers = serviceProvider.listIdentifiers(ListIdentifiersParameters.request().withMetadataPrefix(metadataPrefix));
        while(identifiers.hasNext()){
            Header header = identifiers.next();
            Assert.assertNotNull(header);
            Assert.assertNotNull(header.getIdentifier());
            Assert.assertNotNull(header.getDatestamp());
        }
    }

    @Test
    public void shouldListIdentifiersWithFromDate() throws BadArgumentException {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Iterator<Header> identifiers =
                serviceProvider.listIdentifiers(
                        ListIdentifiersParameters.request().
                                withMetadataPrefix(metadataPrefix).
                                withFrom(new Date(0)));

        while(identifiers.hasNext()){
            Header header = identifiers.next();
            Assert.assertNotNull(header);
            Assert.assertNotNull(header.getIdentifier());
            Assert.assertNotNull(header.getDatestamp());
        }
    }

    @Test
    public void shouldListIdentifiersWithUntilDate() throws BadArgumentException {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Iterator<Header> identifiers =
                serviceProvider.listIdentifiers(
                        ListIdentifiersParameters.request().
                                withMetadataPrefix(metadataPrefix).
                                withUntil(new Date()));

        while(identifiers.hasNext()){
            Header header = identifiers.next();
            Assert.assertNotNull(header);
            Assert.assertNotNull(header.getIdentifier());
            Assert.assertNotNull(header.getDatestamp());
        }
    }

    @Test
    public void shouldReturnEmptyIdentifiersListWithUntilDate() throws BadArgumentException {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Iterator<Header> identifiers =
                serviceProvider.listIdentifiers(
                        ListIdentifiersParameters.request().
                                withMetadataPrefix(metadataPrefix).
                                withUntil(new Date(0)));

        Assert.assertFalse(identifiers.hasNext());
    }

    @Test
    public void shouldReturnEmptyIdentifiersListWithFromDate() throws BadArgumentException {
        ServiceProvider serviceProvider = new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(endpoint)));
        Iterator<Header> identifiers =
                serviceProvider.listIdentifiers(
                        ListIdentifiersParameters.request().
                                withMetadataPrefix(metadataPrefix).
                                withFrom(new Date()));

        Assert.assertFalse(identifiers.hasNext());
    }

}

package eu.europeana.cloud.service.dps.oaiTester.integration;

import com.lyncode.xoai.model.oaipmh.MetadataFormat;
import com.lyncode.xoai.model.oaipmh.Record;
import com.lyncode.xoai.model.oaipmh.Verb;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.client.OAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.BadArgumentException;
import com.lyncode.xoai.serviceprovider.exceptions.CannotDisseminateFormatException;
import com.lyncode.xoai.serviceprovider.exceptions.IdDoesNotExistException;
import com.lyncode.xoai.serviceprovider.exceptions.OAIRequestException;
import com.lyncode.xoai.serviceprovider.model.Context;
import com.lyncode.xoai.serviceprovider.parameters.GetRecordParameters;
import com.lyncode.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import com.lyncode.xoai.serviceprovider.parameters.ListRecordsParameters;
import com.lyncode.xoai.serviceprovider.parameters.Parameters;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by pwozniak on 1/30/18
 */
@RunWith(Parameterized.class)
public class IntegrationTest {


    @Parameterized.Parameters
    public static Collection<String> data() {
        return Arrays.asList(
                "http://www.wbc.poznan.pl/dlibra/oai-pmh-repository.xml");
    }


    private String endpoint;

    public IntegrationTest(String endpoint) {
        this.endpoint = endpoint;
    }

    @Test
    public void test1() throws BadArgumentException, IdDoesNotExistException, CannotDisseminateFormatException, OAIRequestException, IOException, TransformerConfigurationException {
        //
        OAIClient client = new HttpOAIClient(endpoint);
        //
        final Context context = new Context()
                .withOAIClient(client)
                .withBaseUrl(endpoint)
                .withMetadataTransformer("oai_dc", Context.KnownTransformer.OAI_DC);
        //
        ServiceProvider serviceProvider = new ServiceProvider(context);

        Iterator<MetadataFormat> metadataFormats = serviceProvider.listMetadataFormats();
        while(metadataFormats.hasNext()){
            MetadataFormat format = metadataFormats.next();
            GetRecordParameters params = new GetRecordParameters().withIdentifier("oai:www.wbc.poznan.pl:331136_1").withMetadataFormatPrefix(format.getMetadataPrefix());
            InputStream is = client.execute(Parameters.parameters().withVerb(Verb.Type.GetRecord).include(params));
            //
            StringWriter writer = new StringWriter();
            IOUtils.copy(is, writer, Charset.defaultCharset());
            //
            Record record = serviceProvider.getRecord(GetRecordParameters.request().withIdentifier("oai:www.wbc.poznan.pl:331136").withMetadataFormatPrefix(format.getMetadataPrefix()));
        }
    }

    private int countRecordsNumber(){

        return 0;
    }
}

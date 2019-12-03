package eu.europeana.cloud.service.dps.rest.oaiharvest.schema;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.rest.oaiharvest.OAIHelper;
import eu.europeana.cloud.service.dps.rest.oaiharvest.OAIItem;
import org.dspace.xoai.model.oaipmh.MetadataFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;


/**
 * Created by Tarek on 5/4/2018.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({AllSchemasHandler.class})
public class AllSchemasHandlerTest {

    public static final String EDM = "EDM";
    public static final String RDF = "RDF";
    public static final String OAI_DC = "OAI_DC";
    private OAIItem  oaiItem;
    private OAIHelper oaiHelper;
    Iterator<MetadataFormat> iterator;

    @Before
    public void init() throws Exception {
        oaiItem = new OAIItem();
        oaiHelper = Mockito.mock(OAIHelper.class);
        PowerMockito.whenNew(OAIHelper.class).withAnyArguments().thenReturn(oaiHelper);

        MetadataFormat metadataFormat1 = new MetadataFormat();
        metadataFormat1.withMetadataPrefix(EDM);

        MetadataFormat metadataFormat2 = new MetadataFormat();
        metadataFormat2.withMetadataPrefix(RDF);

        MetadataFormat metadataFormat3 = new MetadataFormat();
        metadataFormat3.withMetadataPrefix(OAI_DC);

        iterator = Mockito.mock(Iterator.class);
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(metadataFormat1, metadataFormat2, metadataFormat3);

    }

    @Test
    public void shouldReturnAllSchemasWithNoExcludedSchemas() throws Exception {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();

        oaiItem.setSourceDetails(oaipmhHarvestingDetails);
        when(oaiHelper.listSchemas()).thenReturn(iterator);
        AllSchemasHandler allSchemasHandler = new AllSchemasHandler();
        Set<String> schemas = allSchemasHandler.getSchemas(oaiItem);
        assertNotNull(schemas);
        assertEquals(3, schemas.size());
    }

    @Test
    public void shouldReturnAllSchemasWithExcludedSchemas() throws Exception {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        oaipmhHarvestingDetails.setExcludedSchemas(new HashSet<>(Arrays.asList(OAI_DC, RDF)));

        oaiItem.setSourceDetails(oaipmhHarvestingDetails);
        when(oaiHelper.listSchemas()).thenReturn(iterator);
        AllSchemasHandler allSchemasHandler = new AllSchemasHandler();
        Set<String> schemas = allSchemasHandler.getSchemas(oaiItem);
        assertNotNull(schemas);
        assertEquals(1, schemas.size());
        assertEquals(EDM,schemas.iterator().next());
    }
}
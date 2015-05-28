package eu.europeana.cloud.service.dps.storm.transform.text;

import eu.europeana.cloud.service.dps.storm.transform.text.oai.OaiExtractionMethods;
import eu.europeana.cloud.service.dps.storm.transform.text.pdf.PdfExtractionMethods;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class FactoryTest 
{
    @Test
    public void getPdfExtractorsTest()
    {
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR, 
                TextExtractorFactory.getExtractor("PDF", "TIKA_EXTRACTOR").getExtractorMethod());
        
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR, 
                TextExtractorFactory.getExtractor("PDF", "defaultExtractor").getExtractorMethod());
        
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR, 
                TextExtractorFactory.getExtractor("PDF", "fdsafdsaf").getExtractorMethod());
        
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR,   //TODO: jpod is not implemented
                TextExtractorFactory.getExtractor("pdf", "jpod_extractor").getExtractorMethod());
        
        assertEquals(PdfExtractionMethods.PDFBOX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("PdF", "PdfBox_Extractor").getExtractorMethod());
    }
    
    @Test
    public void getOaiExtractorsTest()
    {
        assertEquals(OaiExtractionMethods.DC, 
                TextExtractorFactory.getExtractor("OAI", "DC").getExtractorMethod());
        
        assertEquals(OaiExtractionMethods.DC, 
                TextExtractorFactory.getExtractor("OAI", "fdsfs").getExtractorMethod());
        
        assertEquals(OaiExtractionMethods.DC, 
                TextExtractorFactory.getExtractor("OaI", "").getExtractorMethod());
    }
    
    @Test
    public void UnsupportedRepresentationTest()
    {
        assertNull(TextExtractorFactory.getExtractor("UNSUPPORTED", "TIKA_EXTRACTOR"));
        
        assertNull(TextExtractorFactory.getExtractor("fdagfda", "TIKA_EXTRACTOR"));
        
        assertNull(TextExtractorFactory.getExtractor("fdagfda", "dssss"));
        
        assertNull(TextExtractorFactory.getExtractor("", "dssss"));
        
        assertNull(TextExtractorFactory.getExtractor(null, "dssss"));
    }
}

package eu.europeana.cloud.service.dps.storm.transform.text;

import eu.europeana.cloud.service.dps.storm.transform.text.oai.OaiExtractionMethods;
import eu.europeana.cloud.service.dps.storm.transform.text.pdf.PdfExtractionMethods;
import eu.europeana.cloud.service.dps.storm.transform.text.txt.TxtExtractionMethods;
import eu.europeana.cloud.service.dps.storm.transform.text.edm.EdmExtractionMethods;
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
        
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR, 
                TextExtractorFactory.getExtractor("PDF", null).getExtractorMethod());
        
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR,   //TODO: jpod is not implemented
                TextExtractorFactory.getExtractor("pdf", "jpod_extractor").getExtractorMethod());
        
        assertEquals(PdfExtractionMethods.PDFBOX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("PdF", "PdfBox_Extractor").getExtractorMethod());
    }
    
    @Test
    public void getOaiExtractorsTest()
    {
        assertEquals(OaiExtractionMethods.DC_EXTRACTOR, 
                TextExtractorFactory.getExtractor("OAI", "DC_Extractor").getExtractorMethod());
        
        assertEquals(OaiExtractionMethods.DC_EXTRACTOR, 
                TextExtractorFactory.getExtractor("OAI", "fdsfs").getExtractorMethod());
        
        assertEquals(OaiExtractionMethods.DC_EXTRACTOR, 
                TextExtractorFactory.getExtractor("OaI", "").getExtractorMethod());
        
        assertEquals(OaiExtractionMethods.DC_EXTRACTOR, 
                TextExtractorFactory.getExtractor("oaI", null).getExtractorMethod());
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
    
    @Test
    public void getTxtExtractorsTest()
    {
        assertEquals(TxtExtractionMethods.READ_FILE_EXTRACTOR, 
                TextExtractorFactory.getExtractor("TXT", "Read_File_Extractor").getExtractorMethod());
        
        assertEquals(TxtExtractionMethods.READ_FILE_EXTRACTOR, 
                TextExtractorFactory.getExtractor("tXT", "fdsfs").getExtractorMethod());
        
        assertEquals(TxtExtractionMethods.READ_FILE_EXTRACTOR, 
                TextExtractorFactory.getExtractor("txt", "").getExtractorMethod());
        
        assertEquals(TxtExtractionMethods.READ_FILE_EXTRACTOR, 
                TextExtractorFactory.getExtractor("txt", null).getExtractorMethod());
    }
    
    @Test
    public void getEdmExtractorsTest()
    {
        assertEquals(EdmExtractionMethods.JIBX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("EDM", "JIBX_extractor").getExtractorMethod());
        
        assertEquals(EdmExtractionMethods.JIBX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("eDM", "fdsfs").getExtractorMethod());
        
        assertEquals(EdmExtractionMethods.JIBX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("edm", "").getExtractorMethod());
        
        assertEquals(EdmExtractionMethods.JIBX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("EDm", null).getExtractorMethod());
    }
}

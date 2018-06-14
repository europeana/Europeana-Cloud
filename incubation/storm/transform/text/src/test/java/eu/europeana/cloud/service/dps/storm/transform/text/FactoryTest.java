package eu.europeana.cloud.service.dps.storm.transform.text;

import eu.europeana.cloud.service.dps.storm.transform.text.edm.EdmExtractionMethods;
import eu.europeana.cloud.service.dps.storm.transform.text.oai.OaiExtractionMethods;
import eu.europeana.cloud.service.dps.storm.transform.text.pdf.PdfExtractionMethods;
import eu.europeana.cloud.service.dps.storm.transform.text.txt.TxtExtractionMethods;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
                TextExtractorFactory.getExtractor("PDF", "TIKA_EXTRACTOR").getExtractionMethod());
        
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR, 
                TextExtractorFactory.getExtractor("PDF", "defaultExtractor").getExtractionMethod());
        
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR, 
                TextExtractorFactory.getExtractor("PDF", "fdsafdsaf").getExtractionMethod());
        
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR, 
                TextExtractorFactory.getExtractor("PDF", null).getExtractionMethod());
        
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR,   //TODO: jpod is not implemented
TextExtractorFactory.getExtractor("pdf", "jpod_extractor").getExtractionMethod());
        
        assertEquals(PdfExtractionMethods.PDFBOX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("PdF", "PdfBox_Extractor").getExtractionMethod());
    }
    
    @Test
    public void getOaiExtractorsTest()
    {
        assertEquals(OaiExtractionMethods.DC_EXTRACTOR, 
                TextExtractorFactory.getExtractor("OAI", "DC_Extractor").getExtractionMethod());
        
        assertEquals(OaiExtractionMethods.DC_EXTRACTOR, 
                TextExtractorFactory.getExtractor("OAI", "fdsfs").getExtractionMethod());
        
        assertEquals(OaiExtractionMethods.DC_EXTRACTOR, 
                TextExtractorFactory.getExtractor("OaI", "").getExtractionMethod());
        
        assertEquals(OaiExtractionMethods.DC_EXTRACTOR, 
                TextExtractorFactory.getExtractor("oaI", null).getExtractionMethod());
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
                TextExtractorFactory.getExtractor("TXT", "Read_File_Extractor").getExtractionMethod());
        
        assertEquals(TxtExtractionMethods.READ_FILE_EXTRACTOR, 
                TextExtractorFactory.getExtractor("tXT", "fdsfs").getExtractionMethod());
        
        assertEquals(TxtExtractionMethods.READ_FILE_EXTRACTOR, 
                TextExtractorFactory.getExtractor("txt", "").getExtractionMethod());
        
        assertEquals(TxtExtractionMethods.READ_FILE_EXTRACTOR, 
                TextExtractorFactory.getExtractor("txt", null).getExtractionMethod());
    }
    
    @Test
    public void getEdmExtractorsTest()
    {
        assertEquals(EdmExtractionMethods.JIBX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("EDM", "JIBX_extractor").getExtractionMethod());
        
        assertEquals(EdmExtractionMethods.JIBX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("eDM", "fdsfs").getExtractionMethod());
        
        assertEquals(EdmExtractionMethods.JIBX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("edm", "").getExtractionMethod());
        
        assertEquals(EdmExtractionMethods.JIBX_EXTRACTOR, 
                TextExtractorFactory.getExtractor("EDm", null).getExtractionMethod());
    }
}

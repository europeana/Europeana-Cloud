package eu.europeana.cloud.service.dps.storm.transform.text.pdf;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class PdfExtractorsTest 
{
    @Test
    public void getTikaExtractorTest()
    {
        String[] strings = 
        {
            "TIKA_EXTRACTOR",
            "TIKA_EXTRActor",
            "tika_extractor",
            "Tika_Extractor",
            "TiKa_ExTrAcTor",
            "default_extractor",
            "DefaultExtractor",
            "StillDefaultExtractor",
            "kadfasfak",
            "PdfExtraftor",
            "PdfboxExtractor",
            ""    
        };
        
        PdfExtractionMethods method;
        
        for(String s: strings)
        {
            method = PdfExtractionMethods.TIKA_EXTRACTOR.getMethod(s);
            assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR, method);
        }
        
        method = PdfExtractionMethods.TIKA_EXTRACTOR.getMethod(null);
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR, method);
    }
    
    @Test
    public void getPdfBoxExtractorTest()
    {
        String[] strings = 
        {
            "PDFBOX_EXTRACTOR",
            "pdfbox_extractor",
            "PdfBox_Extractor",
            "PDFbox_Extractor",
            "PDFBOX_EXTractor",
            "Pdfbox_Extractor",
            "PdFbOx_ExTrAcToR"
        };
              
        for(String s: strings)
        {
            PdfExtractionMethods method = PdfExtractionMethods.TIKA_EXTRACTOR.getMethod(s);
            assertEquals(PdfExtractionMethods.PDFBOX_EXTRACTOR, method);
        }
    }
    
    @Test
    public void getJPodExtractorTest()
    {
        String[] strings = 
        {
            "JPOD_EXTRACTOR",
            "jpod_extractor",
            "Jpod_Extractor",
            "JPod_Extractor",
            "JPOD_EXTractor",
            "JpOd_ExTrAcToR"
        };
              
        for(String s: strings)
        {
            PdfExtractionMethods method = PdfExtractionMethods.TIKA_EXTRACTOR.getMethod(s);
            assertEquals(PdfExtractionMethods.JPOD_EXTRACTOR, method);
        }
    }
}

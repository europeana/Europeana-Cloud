package eu.europeana.cloud.service.dps.storm.transform.text.pdf;

import eu.europeana.cloud.service.dps.storm.transform.text.MethodsEnumeration;

/**
 * Supported extraction methods for PDF
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public enum PdfExtractionMethods implements MethodsEnumeration<PdfExtractionMethods>
{
    TIKA_EXTRACTOR,     //default extractor
    JPOD_EXTRACTOR,
    PDFBOX_EXTRACTOR;
    
    @Override
    public PdfExtractionMethods getMethod(String value)
    {
        PdfExtractionMethods ret;
        try
        {
            ret = valueOf(value.toUpperCase());
        }
        catch(IllegalArgumentException | NullPointerException ex)
        {
           return TIKA_EXTRACTOR; //defaul enum constant
        }
        
        return ret;
    }
}

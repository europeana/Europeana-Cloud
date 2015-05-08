package eu.europeana.cloud.service.dps.storm.transform.text;

import eu.europeana.cloud.service.dps.storm.transform.text.oai.DcExtractor;
import eu.europeana.cloud.service.dps.storm.transform.text.oai.OaiExtractionMethods;
import eu.europeana.cloud.service.dps.storm.transform.text.pdf.PdfBoxExtractor;
import eu.europeana.cloud.service.dps.storm.transform.text.pdf.TikaExtractor;
import eu.europeana.cloud.service.dps.storm.transform.text.pdf.PdfExtractionMethods;

/**
 * Factory for select extraction method.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TextExtractorFactory 
{
    /**
     * Retrieve extractor for specific representation by extractor name.
     * @param representationName Name of representation
     * @param extractorName Extractor name
     * @return Instance of extractor or null if representation is not supported
     */
    public static TextExtractor getExtractor(String representationName, String extractorName)
    {     
        switch(SupportedRepresentations.getMethod(representationName))
        {
            case PDF:
                return getPdfExtractor(extractorName);
            case OAI:
                return getOaiExtractor(extractorName);
            case UNSEPPORTED:
            default:
                return null;
        }
    }
    
    /**
     * Retrieve extractor for PDF format.
     * It uses extractors from enum {@link PdfExtractionMethods}.
     * If given extractor is not implemented, than it will be used TIKA_EXTRACTOR.
     * @param extractorName Extractor name
     * @return Instance of extractor
     */
    private static TextExtractor getPdfExtractor(String extractorName)
    {
        PdfExtractionMethods method = PdfExtractionMethods.TIKA_EXTRACTOR.getMethod(extractorName);

        switch(method)
        {
            case PDFBOX_EXTRACTOR:
                return new PdfBoxExtractor();
            case TIKA_EXTRACTOR:
            default:
                return new TikaExtractor();
        }
    }
    
    /**
     * Retrieve extractor pro OAI format.
     * It uses extractors from enum {@link OaiExtractionMethods}.
     * If given extractor is not implemented, than it will be used DC.
     * @param extractorName Extractor name
     * @return Instance of extractor
     */
    private static TextExtractor getOaiExtractor(String extractorName)
    {
        OaiExtractionMethods method = OaiExtractionMethods.DC.getMethod(extractorName);
        
        switch(method)
        {
            case DC:
            default:
                return new DcExtractor();
        }
    }
}

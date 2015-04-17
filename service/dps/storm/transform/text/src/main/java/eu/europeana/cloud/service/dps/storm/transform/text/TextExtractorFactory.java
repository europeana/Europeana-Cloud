package eu.europeana.cloud.service.dps.storm.transform.text;

/**
 * Factory for select extraction method.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TextExtractorFactory 
{
    /**
     * Retrieve extractor by name.
     * It uses extractors from enum {@link ExtractionMethods}.
     * If given extractor is not implemented, than it will be used TIKA_EXTRACTOR.
     * @param extractorName Extractor name
     * @return Instance of extractor
     */
    public static TextExtractor getExtractor(String extractorName)
    {      
        switch(ExtractionMethods.getMethod(extractorName))
        {
            case PDFBOX_EXTRACTOR:
                return new PdfBoxExtractor();
            case TIKA_EXTRACTOR:
            default:
                return new TikaExtractor();
        }
    }
}

package eu.europeana.cloud.service.dps.storm.transform.text;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.slf4j.LoggerFactory;

/**
 * Text extractor for PDF files that uses the Apache PDFBox library
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class PdfBoxExtractor implements TextExtractor
{
    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PdfBoxExtractor.class);
    
    @Override
    public String extractText(InputStream is) 
    {
        PDFParser parser;
        String parsedText = null;
        PDFTextStripper pdfStripper = null;
        PDDocument pdDoc = null;
        COSDocument cosDoc = null;
        
        try 
        {
            parser = new PDFParser(is);
            parser.parse();
            cosDoc = parser.getDocument();
            pdfStripper = new PDFTextStripper();        
            
            pdDoc = new PDDocument(cosDoc);
            parsedText = pdfStripper.getText(pdDoc);    //possible NULL pointer if document is encrypted
        } 
        catch (IOException ex)
        {
            LOGGER.warn("Can not extract text from pdf because:", ex.getMessage());
        } 
        finally 
        {
            try 
            {
                if (cosDoc != null)
                    cosDoc.close();
                if (pdDoc != null)
                    pdDoc.close();
            } 
            catch (IOException ex) 
            {}
        }
        
        return parsedText;
    }

    @Override
    public ExtractionMethods getExtractorMethod() 
    {
        return ExtractionMethods.PDFBOX_EXTRACTOR;
    } 

    @Override
    public Map<String, String> getExtractedMetadata() 
    {
        return null;    //TODO: extract metadata!
    }
}

package eu.europeana.cloud.service.dps.storm.transform.text.pdf;

import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractor;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * Text extractor for PDF files that uses the Apache Tika toolkit
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TikaExtractor implements TextExtractor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TikaExtractor.class);
    
    private Metadata metadata;
    
    @Override
    public String extractText(InputStream is) 
    {
        BodyContentHandler handler = new BodyContentHandler(-1);    // -1 to disable the write limit
        metadata = new Metadata();
        ParseContext pcontext = new ParseContext();
        PDFParser pdfparser = new PDFParser(); 
        
        try 
        {
            pdfparser.parse(is, handler, metadata,pcontext);
        } 
        catch (IOException | SAXException | TikaException ex) 
        {
          LOGGER.warn("Can not extract text from pdf because: " + ex.getMessage()); 
          return null;
        }
        
        return handler.toString();
    }

    @Override
    public PdfExtractionMethods getExtractorMethod() 
    {
        return PdfExtractionMethods.TIKA_EXTRACTOR;
    }

    @Override
    public Map<String, String> getExtractedMetadata() 
    {
        Map<String, String> ret = new HashMap<>();
        for (String name : metadata.names())
        {
            ret.put(name, metadata.get(name));
        }
        
        return ret;
    }   

    @Override
    public String getRepresentationName() 
    {
        return "text-from-pdf";
    }
}

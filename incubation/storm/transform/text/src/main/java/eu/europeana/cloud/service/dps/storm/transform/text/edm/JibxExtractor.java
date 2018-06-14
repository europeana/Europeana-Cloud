package eu.europeana.cloud.service.dps.storm.transform.text.edm;

import eu.europeana.cloud.service.dps.storm.transform.text.MethodsEnumeration;
import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractor;
import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.corelib.edm.utils.SolrConstructor;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.codehaus.jackson.map.ObjectMapper;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * JIBX text extractor for EDM files.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class JibxExtractor implements TextExtractor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JibxExtractor.class);

    private static IBindingFactory bfact;
    static
    {
        try
        {
            //Should be placed in a static block for performance reasons
            bfact = BindingDirectory.getFactory(RDF.class);
        }
        catch(JiBXException ex)
        {
            LOGGER.error("Cannot create the JibX factory because: "+ex.getMessage());
        }
    }
    
    @Override
    public String extractText(InputStream is) 
    {
        if(is == null)
        {
            return null;
        }
        
        try 
        {
            IUnmarshallingContext uctx = bfact.createUnmarshallingContext();
            RDF rdf = (RDF)uctx.unmarshalDocument(is, null);
            
            //excract data from EDM
            Map<String, Object> res = new HashMap<>();
            SolrInputDocument solrDoc = new SolrConstructor().constructSolrDocument(rdf);
            for(Map.Entry<String, SolrInputField> field: solrDoc.entrySet())
            {
                Object o = field.getValue().getValue();
                if(o != null)
                {
                    res.put(field.getKey(), o);
                }
            }
            
            return new ObjectMapper().writeValueAsString(res);
        } 
        catch (JiBXException | IOException ex) 
        {
            LOGGER.warn("Cannot convert EDM to string because: "+ex.getMessage());
        }
        catch (InstantiationException | IllegalAccessException ex)  //builder wants these two exceptions (NetBeans not)
        {
            LOGGER.warn("Cannot convert EDM to string because: "+ex.getMessage());
        }
        catch(NullPointerException ex)  //wrong EDM file
        {
            LOGGER.warn("Cannot convert EDM to string because: not valid EDM");
            return null;
        }
                
        return null;
    }

    @Override
    public MethodsEnumeration getExtractionMethod() 
    {
        return EdmExtractionMethods.JIBX_EXTRACTOR;
    }

    @Override
    public Map<String, String> getExtractedMetadata() 
    {
        return null;
    }

    @Override
    public String getRepresentationName() 
    {
        return "edm_as_json";
    }
}

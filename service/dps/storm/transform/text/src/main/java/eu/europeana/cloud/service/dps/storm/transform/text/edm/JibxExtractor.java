package eu.europeana.cloud.service.dps.storm.transform.text.edm;

import eu.europeana.cloud.service.dps.storm.transform.text.MethodsEnumeration;
import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractor;
import eu.europeana.corelib.edm.utils.MongoConstructor;
import eu.europeana.corelib.definitions.jibx.RDF;
import java.io.InputStream;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;

/**
 *
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
            FullBeanImpl fbi = new MongoConstructor().constructFullBean(rdf);
            
            return new ObjectMapper().writeValueAsString(fbi);
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
            LOGGER.warn("Cannot convert EDM to string because: "+ex.getMessage());
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

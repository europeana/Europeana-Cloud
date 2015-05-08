package eu.europeana.cloud.service.dps.storm.transform.text.oai;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import eu.europeana.cloud.service.dps.storm.transform.text.MethodsEnumeration;
import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractor;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class DcExtractor implements TextExtractor
{
    private final Map<String, String> tags;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(DcExtractor.class);

    public DcExtractor() 
    {
        tags = new HashMap<>();
        tags.put("title", "dc:title");
        tags.put("description", "dc:description");
        tags.put("creator", "dc:creator");
        tags.put("publisher", "dc:publisher");
        tags.put("date", "dc:date");
        tags.put("type", "dc:type");
        tags.put("format", "dc:format");
        tags.put("source", "dc:cource");
        tags.put("language", "dc:language");
    }

    public DcExtractor(Map<String, String> tags) 
    {
        this.tags = tags;
    }

    @Override
    public String extractText(InputStream is) 
    {
        JsonObject ret = new JsonObject();
        SAXBuilder builder = new SAXBuilder();
        try 
        {
            Document document = builder.build(is);
            Element metadataNode = document.getRootElement().getChild("metadata");                
            Element dcNode = metadataNode.getChild("dc", Namespace.getNamespace("http://www.openarchives.org/OAI/2.0/oai_dc/"));

            for(Map.Entry<String, String> tag: tags.entrySet())
            {
                List<Element> list;
                
                //use namespace?
                String[] tagInfo = tag.getValue().split(":", 2);
                if(tagInfo.length > 1)
                {
                    list = dcNode.getChildren(tagInfo[1], dcNode.getNamespace(tagInfo[0]));
                }
                else
                {
                    list = dcNode.getChildren(tagInfo[0]);
                }

                if(list.isEmpty())
                {
                    continue;
                }
                
                //add as array or single value?
                if(list.size() == 1)
                {
                    String s = list.get(0).getTextTrim();
                    if(!s.isEmpty())
                    {
                        ret.addProperty(tag.getKey(), s);
                    }
                }
                else
                {
                    JsonArray array = new JsonArray();
                    for (Element element: list) 
                    {
                        String s = element.getTextTrim();
                        if(!s.isEmpty())
                        {
                            array.add(new JsonPrimitive(s));
                        }
                    }
                    ret.add(tag.getKey(), array);
                }
            }
            
            return new Gson().toJson(ret);
        } 
        catch (JDOMException | IOException ex) 
        {
            LOGGER.warn("Can not extract text from oai-dc because: " + ex.getMessage());
        }
        
        return null;
    }

    @Override
    public MethodsEnumeration getExtractorMethod() 
    {
        return OaiExtractionMethods.DC;
    }

    @Override
    public Map<String, String> getExtractedMetadata() 
    {
        return null;
    }

    @Override
    public String getRepresentationName() 
    {
        return "json-extracted-from-oai-dc";
    }
}

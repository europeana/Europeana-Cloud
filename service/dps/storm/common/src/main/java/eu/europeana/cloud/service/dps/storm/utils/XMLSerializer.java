package eu.europeana.cloud.service.dps.storm.utils;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Util class used to serialize object to XML and deserialize XML back to object.
 *
 * @param <T> class of the objects
 */
public final class XMLSerializer<T> {

    public T deserialize(String xml, Class<T> outputClass) {
        JAXBContext context = null;
        Unmarshaller um = null;

        try {
            context = JAXBContext.newInstance(outputClass);
            um = context.createUnmarshaller();
            return (T) um.unmarshal(new StringReader(xml));
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String serialize(T object, Class<T> inputClass) {
        JAXBContext context = null;
        Marshaller m = null;
        StringWriter sw = new StringWriter();

        try {
            context = JAXBContext.newInstance(inputClass);
            m = context.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            m.marshal(object, sw);
            return sw.toString();
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        finally {
            if (sw != null) {
                try {
                    sw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}

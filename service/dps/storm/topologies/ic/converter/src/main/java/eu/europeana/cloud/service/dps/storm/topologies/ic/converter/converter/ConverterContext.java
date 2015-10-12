package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter;


import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.UnexpectedExtensionsException;

import java.io.IOException;
import java.util.List;

/**
 * Created by Tarek on 8/21/2015.
 */

/**
 * Context for choosing the appropriate Converter
 */
public class ConverterContext {
    private Converter converter;
    public ConverterContext(Converter converter)
    {
        this.converter=converter;
    }

    public void convert(String inputFilePath, String outputFilePath, List<String> properties)throws UnexpectedExtensionsException,IOException
    {
        converter.convert(inputFilePath,outputFilePath,properties);
    }

}

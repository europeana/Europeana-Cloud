package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter;


import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.command.CommandBuilderContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.command.ImageMagicConvertCommandBuilder;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension.ExtensionCheckerContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension.JPGExtensionChecker;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension.TiffExtensionChecker;

/**
 * Service for converting jpg file to tiff file by executing image magic convert shell command
 */
public class ImageMagicJPGToTiff extends ConsoleBasedConverter {
    public ImageMagicJPGToTiff() {
        super(new CommandBuilderContext(new ImageMagicConvertCommandBuilder()), new ExtensionCheckerContext(new JPGExtensionChecker()), new ExtensionCheckerContext(new TiffExtensionChecker()));

    }
}

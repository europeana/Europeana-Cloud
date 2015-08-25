package eu.europeana.cloud.service.dps.examples.tutorial;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import javax.imageio.ImageIO;

/**
 *
 * @author lucasanastasiou
 */
public class ConvertBolt extends AbstractDpsBolt {

    @Override
    public void execute(StormTaskTuple t) {

        try {
            // fetch the image from the input tuple
            ByteArrayInputStream inputImage = t.getFileByteDataAsStream();
            BufferedImage bufferedImage = ImageIO.read(inputImage);
            OutputStream os = new ByteArrayOutputStream();
            // transcoding to jpeg
            ImageIO.write(bufferedImage, "jpg", os);

            t.setFileData(os.toString());
            //emitting to the next bolt
            outputCollector.emit("stream-to-next-bolt", t.toStormTuple());

        } catch (IOException ex) {
            // send notification of error
            this.emitErrorNotification(t.getTaskId(), t.getFileUrl(), ex.getMessage(), "");
            outputCollector.fail(inputTuple);
        }
    }

    @Override
    public void prepare() {
        //
        // your code goes here
        //
    }

}

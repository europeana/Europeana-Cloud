package eu.europeana.cloud.common.utils;

import org.apache.commons.lang3.time.FastDateFormat;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.text.ParseException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;


/**
 * Created by Tarek on 11/30/2017.
 */
public class DateAdapter extends XmlAdapter<String, Date> {
    //This was used based on Metis requirements. ex: 2017-11-23T10:43:26.038Z
    private static final String FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    private static final FastDateFormat FORMATTER = FastDateFormat.getInstance(FORMAT, TimeZone.getTimeZone("UTC"));
    @Override
    public String marshal(Date date) {
        if (date == null) {
            throw new RuntimeException("The revision creation Date shouldn't be null");
        }
        return FORMATTER.format(date);
    }

    @Override
    public Date unmarshal(String stringDate) throws ParseException {
        if (stringDate == null || stringDate.isEmpty()) {
            return null;
        }
        return new Date(Long.parseLong(stringDate));
    }
}
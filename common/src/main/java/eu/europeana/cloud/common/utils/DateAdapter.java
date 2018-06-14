package eu.europeana.cloud.common.utils;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


/**
 * Created by Tarek on 11/30/2017.
 */
public class DateAdapter extends XmlAdapter<String, Date> {
    //This was used based on Metis requirements. ex: 2017-11-23T10:43:26.038Z
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    @Override
    public String marshal(Date date) {
        if (date == null) {
            throw new RuntimeException("The revision creation Date shouldn't be null");
        }
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(date);
    }

    @Override
    public Date unmarshal(String stringDate) throws ParseException {
        if (stringDate == null || stringDate.isEmpty()) {
            return null;
        }
        try {
            return dateFormat.parse(stringDate);
        } catch (ParseException e) {
            throw new ParseException(e.getMessage() + ". The accepted date format is yyyy-MM-dd'T'HH:mm:ss.SSSXXX", e.getErrorOffset());
        }
    }
}
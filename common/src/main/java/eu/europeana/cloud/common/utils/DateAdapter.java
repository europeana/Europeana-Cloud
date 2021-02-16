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

    @Override
    public String marshal(Date date) {
        if (date == null) {
            throw new RuntimeException("The revision creation Date shouldn't be null");
        }
        return ""+date.getTime();
    }

    @Override
    public Date unmarshal(String stringDate) {
        if (stringDate == null || stringDate.isEmpty()) {
            return null;
        }
        return new Date(Long.parseLong(stringDate));
    }
}
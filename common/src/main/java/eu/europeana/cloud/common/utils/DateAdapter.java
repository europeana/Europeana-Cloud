package eu.europeana.cloud.common.utils;

import jakarta.xml.bind.annotation.adapters.XmlAdapter;

import java.util.Date;

/**
 * Created by Tarek on 11/30/2017. Class uses number of ms from year 1970 as date representation. It is caused by the fact, that
 * this class is used only in application clients based on Jersey, and these client must conform to date representation that is
 * used by REST applications based on spring MVC: AAS, UIS, MCS, DPS.
 */
public class DateAdapter extends XmlAdapter<String, Date> {

  @Override
  public String marshal(Date date) {
    if (date == null) {
      throw new RuntimeException("The revision creation Date shouldn't be null");
    }
    return String.valueOf(date.getTime());
  }

  @Override
  public Date unmarshal(String stringDate) {
    if (stringDate == null || stringDate.isEmpty()) {
      return null;
    }
    return new Date(Long.parseLong(stringDate));
  }
}

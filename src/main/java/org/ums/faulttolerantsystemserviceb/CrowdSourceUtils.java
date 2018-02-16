package org.ums.faulttolerantsystemserviceb;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Monjur-E-Morshed on 12-Feb-18.
 */
public class CrowdSourceUtils {
  public static List<Checkins> convertFromIterableToList(Iterable<Checkins> pCheckins){
    List<Checkins> checkinsList = new ArrayList<>();
    pCheckins.forEach(checkinsList::add);
    return checkinsList;
  }
}

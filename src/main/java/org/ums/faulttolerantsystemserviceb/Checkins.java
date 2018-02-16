package org.ums.faulttolerantsystemserviceb;

import javax.persistence.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Monjur-E-Morshed on 12-Feb-18.
 */
@Entity
@Table(name="gowalla_checkins")
public class Checkins {
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private Long id;

  private Long userId;

  private String date;

  private Timestamp time;

  private Double latitude;

  private Double longitude;

  private Integer naiveLal;

  private Integer bisectionLal;

  @Transient
  private Date dateObj;

  public Checkins() {
  }

  public Date getDateObj() throws Exception{
    return new SimpleDateFormat("yyyy-MM-dd").parse(date);
  }

  public void setDateObj(Date pDateObj) {
    dateObj = pDateObj;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long pId) {
    id = pId;
  }

  public Long getUserId() {
    return userId;
  }

  public void setUserId(Long pUserId) {
    userId = pUserId;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String pDate) {
    date = pDate;
  }

  public Timestamp getTime() {
    return time;
  }

  public void setTime(Timestamp pTime) {
    time = pTime;
  }

  public Double getLatitude() {
    return latitude;
  }

  public void setLatitude(Double pLatitude) {
    latitude = pLatitude;
  }

  public Double getlongitude() {
    return longitude;
  }

  public void setlongitude(Double plongitude) {
    longitude = plongitude;
  }

  public Integer getNaiveLal() {
    return naiveLal;
  }

  public void setNaiveLal(Integer pNaiveLal) {
    naiveLal = pNaiveLal;
  }

  public Integer getBisectionLal() {
    return bisectionLal;
  }

  public void setBisectionLal(Integer pBisectionLal) {
    bisectionLal = pBisectionLal;
  }
}

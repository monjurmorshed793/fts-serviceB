package org.ums.faulttolerantsystemserviceb;

import javax.persistence.*;

/**
 * Created by Monjur-E-Morshed on 13-Feb-18.
 */
@Entity
@Table(name = "bisection_partition")
public class Partition {
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  @Column(name = "id")
  private Integer id;

  @Column(name = "partition_value")
  private Integer partition;

  public Partition() {
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer pId) {
    id = pId;
  }

  public Integer getPartition() {
    return partition;
  }

  public void setPartition(Integer pPartition) {
    partition = pPartition;
  }
}

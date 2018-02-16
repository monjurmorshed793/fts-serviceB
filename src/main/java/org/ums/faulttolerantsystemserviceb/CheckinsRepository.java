package org.ums.faulttolerantsystemserviceb;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by Monjur-E-Morshed on 12-Feb-18.
 */
@Repository
public interface CheckinsRepository extends CrudRepository<Checkins, Long> {
  @Query("select f from Checkins f group by f.date")
  List<Checkins> findAllGroupByDate();
}

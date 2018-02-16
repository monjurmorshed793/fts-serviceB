package org.ums.faulttolerantsystemserviceb;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Monjur-E-Morshed on 13-Feb-18.
 */
@Repository
public interface PartitionRepository extends JpaRepository<Partition, Integer> {

}

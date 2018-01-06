package org.ums.faulttolerantsystemserviceb;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class ServiceBAspect {
    private static final Logger logger = LoggerFactory.getLogger(ServiceBAspect.class);

    @After("execution(* org.ums.faulttolerantsystemserviceb.*..*(org.apache.kafka.clients.consumer.ConsumerRecord)) && args(cr)")
    public void after(JoinPoint joinPoint, ConsumerRecord<?,?> cr)throws  Throwable{
        JoinPoint point=joinPoint;
        logger.info("From aspect");
     logger.info(cr.key().toString());
    }
}

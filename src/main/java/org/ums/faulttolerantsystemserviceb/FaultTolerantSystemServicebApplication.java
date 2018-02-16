package org.ums.faulttolerantsystemserviceb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.circuitbreaker.EnableCircuitBreaker;
import org.springframework.http.RequestEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.websocket.server.PathParam;
import java.util.List;
import java.util.stream.IntStream;

@SpringBootApplication
@RestController("/")
@EnableCircuitBreaker
public class FaultTolerantSystemServicebApplication {

    public static String SERVICE_NAME="serviceB";
    public static String SERVICE_TRACKER="tracker";
    public static String KAFKA_TOPIC="my_topic";
    public static Logger logger = LoggerFactory.getLogger(FaultTolerantSystemServicebApplication.class);

    @Autowired
    private KafkaTemplate<String, String> template;
  @Autowired
  CheckinsRepository mCheckinsRepository;
  @Autowired
  NativePartitionService mNativePartitionService;
  @Autowired
  PartitionRepository mPartitionRepository;

	public static void main(String[] args)  {
		SpringApplication.run(FaultTolerantSystemServicebApplication.class, args);

	}

	@GetMapping("/")
    public String home(RequestEntity requestEntity) throws Exception{
        RestTemplate restTemplate = new RestTemplate();
        String message=restTemplate.getForObject("http://localhost:8081", String.class);

        return message;
    }

    @GetMapping("/service-b")
    public boolean getReponse(@RequestParam("number") Integer pNumber)throws Exception{
      return syncrhonousServiceCall(pNumber);
    }


    public boolean reliable(@RequestParam("number") Integer pNumber){
	    return false;
    }
  @HystrixCommand(fallbackMethod = "reliable")
  private boolean syncrhonousServiceCall(@RequestParam("number") Integer pNumber) {
    RestTemplate restTemplate = new RestTemplate();
    boolean serviceCStatus=false;
    boolean serviceDStatus=false;
    Iterable<Checkins> checkins = mCheckinsRepository.findAll();
    List<Checkins> checkinsList = CrowdSourceUtils.convertFromIterableToList(checkins);
    checkinsList.sort((o1,o2)->o1.getDate().compareTo(o2.getDate()));
    //Map<Date, List<Checkins>> listMap = mNativePartitionService.getGroupBy(checkinsList);

  // mNativePartitionService.assignPartitions(checkinsList, 500);
    for(int i=1; i<=pNumber; i++){
      serviceCStatus=restTemplate.getForObject("http://localhost:8084/service-c", Boolean.class);
      serviceDStatus=restTemplate.getForObject("http://localhost:8011/service-d", Boolean.class);
    }

    return serviceCStatus==true && serviceDStatus==true;
  }




  @GetMapping("/service-b/parallel")
  public boolean getReponseParallel(@RequestParam("number") Integer pNumber)throws Exception{
    RestTemplate restTemplate = new RestTemplate();


    IntStream.range(0,pNumber).parallel().forEach(i->{
      boolean serviceCStatus=restTemplate.getForObject("http://localhost:8084/service-c", Boolean.class);
      boolean serviceDStatus=restTemplate.getForObject("http://localhost:8011/service-d", Boolean.class);
      if(serviceCStatus==false || serviceDStatus==false)
        throw new NullPointerException();
    });

    return true;
  }

	@KafkaListener(topics = "service-b")
    public void listen(ConsumerRecord<?,?> cr) throws Exception{
	  String key=cr.key().toString();
            logger.info(cr.key().toString());
            logger.info(cr.toString());
            int keyValue = Integer.parseInt(key);
     for(int i=1; i<=Integer.parseInt(key);i++){
       ObjectMapper mapper = new ObjectMapper();

       ServiceStatus receivedService = mapper.readValue(cr.value().toString(), ServiceStatus.class);

       ServiceStatus serviceStatus = new ServiceStatus();
       serviceStatus.setServiceId("serviceB"+i);
       serviceStatus.setParentServiceId(receivedService.getServiceId());
       serviceStatus.setStatus(true);
       String serviceStatusJsonObject = mapper.writeValueAsString(serviceStatus);
       template.send("service-c", ""+i, serviceStatusJsonObject);
       template.send("service-d", ""+i, serviceStatusJsonObject);
       template.send(SERVICE_TRACKER, SERVICE_NAME, serviceStatusJsonObject);
       if (!getSuccessResponse(serviceStatus.getServiceId()))
         throw new NullPointerException();
     }

    Thread.sleep(500);

  }

  public boolean getSuccessResponse(String serviceName) throws InterruptedException {
        Boolean serviceExecutionStatus=false;
        RestTemplate restTemplate = new RestTemplate();
        for(int i=0; i<50; i++){
            serviceExecutionStatus=restTemplate.getForObject("http://localhost:8099/service?service-name="+serviceName+"&service-number=2", Boolean.class);
            if(serviceExecutionStatus)
                break;
            Thread.sleep(500);
        }
    return serviceExecutionStatus;
    }


}

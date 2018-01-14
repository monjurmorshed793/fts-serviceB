package org.ums.faulttolerantsystemserviceb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.RequestEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
@RestController("/")
public class FaultTolerantSystemServicebApplication {

    public static String SERVICE_NAME="serviceB";
    public static String SERVICE_TRACKER="tracker";
    public static String KAFKA_TOPIC="my_topic";
    public static Logger logger = LoggerFactory.getLogger(FaultTolerantSystemServicebApplication.class);

    @Autowired
    private KafkaTemplate<String, String> template;

	public static void main(String[] args)  {
		SpringApplication.run(FaultTolerantSystemServicebApplication.class, args);

	}

	@GetMapping("/")
    public String home(RequestEntity requestEntity) throws Exception{
        RestTemplate restTemplate = new RestTemplate();
        String message=restTemplate.getForObject("http://localhost:8081", String.class);

        return message;
    }


	@KafkaListener(topics = "my_topic")
    public void listen(ConsumerRecord<?,?> cr) throws Exception{
	    if(cr.key().toString().equals("serviceB")){
            logger.info(cr.key().toString());
            logger.info(cr.toString());

        ObjectMapper mapper = new ObjectMapper();

        ServiceStatus receivedService = mapper.readValue(cr.value().toString(), ServiceStatus.class);

            ServiceStatus serviceStatus = new ServiceStatus();
            serviceStatus.setServiceId("serviceB");
            serviceStatus.setParentServiceId(receivedService.getServiceId());
        serviceStatus.setStatus(true);
            String serviceStatusJsonObject = mapper.writeValueAsString(serviceStatus);
        template.send("my_topic", "serviceC", serviceStatusJsonObject);
        template.send("my_topic", "serviceD", serviceStatusJsonObject);
        template.send(SERVICE_TRACKER, SERVICE_NAME, serviceStatusJsonObject);
        if (!getSuccessResponse())
          throw new NullPointerException();
      }


    }

  public boolean getSuccessResponse() throws InterruptedException {
        Boolean serviceExecutionStatus=false;
        RestTemplate restTemplate = new RestTemplate();
        for(int i=0; i<15; i++){
            serviceExecutionStatus=restTemplate.getForObject("http://localhost:8099/service?service-name=serviceB&service-number=2", Boolean.class);
            if(serviceExecutionStatus)
                break;
            Thread.sleep(500);
        }
    return serviceExecutionStatus;
    }


}

package maven;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.*;

public class Consumer {

	public static void main(String[] args)
	{
		final Logger logger = LoggerFactory.getLogger(Consumer.class);
		logger.info("--- CONSOLE INICIADO ---");
		  
		  try 
		  {
			var consumer = new KafkaConsumer<String, String>(properties());
			consumer.subscribe(Arrays.asList("EXEMPLO_TOPICO"));

		    while (true) {
		        var records = consumer.poll(Duration.ofMillis(10));
		        for (ConsumerRecord<String, String> registro : records) 
		        {
		            logger.info("--------------");
		            logger.info("RECEBENDO MENSAGENS");
		            System.out.println("Chave:" +registro.key());
		            System.out.println("Valor:" + registro.value());
		        }
		 
		    }
		  }
		  catch(Exception e) 
		  {
			  throw e;
		  }
	}

	private static Properties properties() 
	{
		 var properties = new Properties();
		 	properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		 	properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		 	properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		 	properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "MEU_TOPICO");
		 	//properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earLiest");
		 	return properties;
	}
	
}

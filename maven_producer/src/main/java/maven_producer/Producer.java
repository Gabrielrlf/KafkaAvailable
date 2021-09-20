package maven_producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		 var producer = new KafkaProducer<String,String>(properties());
		    var key = "TESTE KEY";
		    var value = "TESTE";
		    var record = new ProducerRecord<String, String>("TESTE_TOPICO", key, value);
		    Callback callback = (data, ex) -> {
		        if (ex != null) {
		            ex.printStackTrace();
		            return;
		        }
		        System.out.println("Mensagem enviada com sucesso para: " + data.topic() + " | partition " + data.partition() + "| offset " + data.offset() + "| tempo " + data.timestamp());
		    };
		    
		    producer.send(record, callback).get();
	}
	
	private static Properties properties() {
	    var properties = new Properties();
	    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    return properties;
	}

}

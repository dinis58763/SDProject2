package tp2.impl.service.rest;

import java.util.List;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tp2.api.service.java.Directory;
import tp2.impl.kafka.KafkaPublisher;
import tp2.impl.kafka.KafkaSubscriber;
import tp2.impl.kafka.KafkaUtils;
import tp2.impl.service.rest.util.CustomLoggingFilter;
import tp2.impl.service.rest.util.GenericExceptionMapper;

public class RepDirServer extends AbstractRestServer {
	
	private static Logger Log = Logger.getLogger(RepDirServer.class.getName());	
	
	// private static final String FROM_BEGINNING = "earliest";
	// private static final String TOPIC = "topic";
	static final int KAFKA_PORT = 9092;
	// static final String KAFKA_BROKERS = "localhost:9092"; // For testing locally
	// private static final String KAFKA_BROKERS = "kafka:9092"; // When running in docker container...
	
	// final KafkaPublisher sender;
	// final KafkaSubscriber receiver;
	static int MAX_NUM_THREADS = 3;

	public RepDirServer(int port) {	
		super(Log, Directory.SERVICE_NAME, port);
		// this.sender = KafkaPublisher.createPublisher(KAFKA_BROKERS);
		// this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
		// System.out.println("CHEGUEI REP DIR SERVER CONSTRUCTOR ---->>>> ");	
		// publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
		// System.out.println("CHEGUEI REP DIR SERVER CONSTRUCTOR PUBLISHER ---->>>> " + publisher);	
		// subscriber = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
		// System.out.println("CHEGUEI REP DIR SERVER CONSTRUCTOR SUBSCRIBER ---->>>> " + subscriber);	
		// subscriber.start(false, this);  --> implements RecordProcessor {
		// KafkaUtils.createTopic(TOPIC, 1, 1);	
		// System.out.println("CHEGUEI REP DIR SERVER CONSTRUCTOR TOPIC ---->>>> " + TOPIC);	
	}
	
	@Override
	void registerResources(ResourceConfig config) {
		System.out.println("CHEGUEI RESOURCE CONFIG REP DIR SERVER ---->>>> ");
		config.register( RepDirResources.getInstance() ); // DirectyResource --> organises the information to the new class 
		config.register( GenericExceptionMapper.class );		
		config.register( CustomLoggingFilter.class);
	}

	public static void main(String[] args) throws Exception {
		new RepDirServer(KAFKA_PORT).start();
	}
}

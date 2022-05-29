package tp2.impl.service.rest;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tp2.api.service.java.Directory;
import tp2.impl.kafka.KafkaPublisher;
import tp2.impl.service.rest.util.CustomLoggingFilter;
import tp2.impl.service.rest.util.GenericExceptionMapper;
import tp2.impl.sync.SyncPoint;
import util.Debug;
import util.Token;

public class DirectoryRestServer extends AbstractRestServer {
	
	public static final int PORT = 4567; // 9092
	private static Logger Log = Logger.getLogger(DirectoryRestServer.class.getName());	
	
	static final String TOPIC = "topic";
	// static final String KAFKA_BROKERS = "localhost:9092"; // For testing locally
	static final String KAFKA_BROKERS = "kafka:9092"; // When running in docker container...
	static Long version = -1L;
	
	final KafkaPublisher publisher;
	
	public DirectoryRestServer(int port) {	
		super(Log, Directory.SERVICE_NAME, port);		
		publisher = null;
		// SyncPoint.getInstance().waitForResult(version);
	}
	
	public DirectoryRestServer() {	
		super(Log, Directory.SERVICE_NAME, PORT);		
		publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
		// SyncPoint.getInstance().waitForResult(version);
	}
	
	public long send( String msg) {
		long offset = publisher.publish(TOPIC, msg +  System.currentTimeMillis());
		if (offset >= 0)
			System.out.println("Message published with sequence number: " + offset);
		else
			System.err.println("Failed to publish message");
		return offset;
	}
	
	@Override
	void registerResources(ResourceConfig config) {
		config.register( DirectoryResources.class ); 
		config.register( GenericExceptionMapper.class );
		
		config.register( CustomLoggingFilter.class);
	}
	
	public static void main(String[] args) throws Exception {
		
		Debug.setLogLevel( Level.INFO, Debug.TP1);
		
		Token.set( args.length > 0 ? args[0] : "");
		
		new DirectoryRestServer(PORT).start();
		
		SyncPoint.getInstance().waitForResult(version);

		var msg = args.length > 0 ? args[0] : "";
		var sender = new DirectoryRestServer();
		
		for (;;) {
			sender.send(msg + System.currentTimeMillis() );
			Thread.sleep(1000);
		}
	}		
}
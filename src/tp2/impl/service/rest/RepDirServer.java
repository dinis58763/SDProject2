package tp2.impl.service.rest;

import java.util.List;
import tp2.impl.kafka.KafkaSubscriber;


public class RepDirServer {
	
	private static final String FROM_BEGINNING = "earliest";

	public static void main(String[] args) {
		
		var subscriber = KafkaSubscriber.createSubscriber(DirectoryRestServer.KAFKA_BROKERS, List.of(DirectoryRestServer.TOPIC),
				FROM_BEGINNING);

		subscriber.start(true, (r) -> {
			System.out.printf("SeqN: %s %d %s\n", r.topic(), r.offset(), r.value());
		});
		// Sync instance --> SyncPoint.getInstance(); 
	}
}

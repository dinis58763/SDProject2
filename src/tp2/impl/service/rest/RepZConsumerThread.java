package tp2.impl.service.rest;

import static tp2.impl.clients.Clients.FilesClients;

import java.util.List;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import jakarta.inject.Singleton;
import tp2.api.FileInfo;
import tp2.api.service.java.Directory;
import tp2.api.service.java.RepDirectory;
import tp2.api.service.java.Result.ErrorCode;
import tp2.api.service.rest.RepRestDirectory;
import tp2.api.service.rest.RestDirectory;
import tp2.impl.kafka.KafkaPublisher;
import tp2.impl.kafka.KafkaSubscriber;
import tp2.impl.kafka.KafkaUtils;
import tp2.impl.kafka.RecordProcessor;
import tp2.impl.service.common.JavaDirectory;
import tp2.impl.sync.SyncPoint;

@Singleton
public class RepZConsumerThread extends Thread implements RecordProcessor {
	
	private static Logger Log = Logger.getLogger(RepZConsumerThread.class.getName());

	static final String FROM_BEGINNING = "earliest";

	KafkaSubscriber receiver;
	SyncPoint<String> sync;
	
	public RepZConsumerThread() {
		receiver = KafkaSubscriber.createSubscriber(RepDirResources.KAFKA_BROKERS, 
			List.of(RepDirResources.TOPIC), FROM_BEGINNING);
		consume();
		sync = SyncPoint.getInstance();
	}
	
	private void consume() {
		System.out.println("CHEGUEI CONSUME REP DIR RESOURCES ---->>>> ");
		receiver.start(true, (r) -> {
			onReceive(r);
		});
	}
	
	@Override
	public void onReceive(ConsumerRecord<String, String> r) {
		// TODO Auto-generated method stub
		var version = r.offset();
		// SyncPoint.getInstance().setVersion(version++);
		System.out.printf("Processing version of Consumer Record: (%d, ---->>>> ,%s)\n",version, r.value());

		var result = r.value();
		System.out.println("CONSUME REP DIR RESOURCES RESULT VALUE ---->>>> " + result);
		sync.setResult( version, result);
		System.out.printf("SeqN: %s %d %s\n", r.topic(), r.offset(), r.value());
	}
}

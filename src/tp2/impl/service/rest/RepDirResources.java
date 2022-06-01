package tp2.impl.service.rest;

import java.time.Duration;
import java.util.List;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import tp2.api.FileInfo;
import tp2.api.service.java.RepDirectory;
import tp2.api.service.java.Result;
import tp2.api.service.rest.RepRestDirectory;
import tp2.impl.kafka.KafkaPublisher;
import tp2.impl.kafka.KafkaSubscriber;
import tp2.impl.kafka.KafkaUtils;
import tp2.impl.kafka.RecordProcessor;
import tp2.impl.service.common.JavaDirectory;
import tp2.impl.sync.SyncPoint;

@Singleton
public class RepDirResources extends Thread implements RepRestDirectory, RecordProcessor {
	// public class TotalOrderExecutor extends Thread implements RecordProcessor 
	static final String FROM_BEGINNING = "earliest";
	// static final String TOPIC = "single_partition_topic";
	private static final String TOPIC = "topic";
	// static final String KAFKA_BROKERS = "localhost:9092";
	static final String KAFKA_BROKERS = "kafka:9092";
	
	static int MAX_NUM_THREADS = 3;
	
	final RepThreadsResources rep;
	final KafkaPublisher sender;
	final KafkaSubscriber receiver;
	final KafkaUtils kafkaUtils = new KafkaUtils();
	final RepDirectory impl;
	
	private static Logger Log = Logger.getLogger(RepDirResources.class.getName());
	private static RepDirResources instance;
	
	@SuppressWarnings("static-access")
	public RepDirResources() {
		kafkaUtils.createTopic(TOPIC,1,1);
		sender = KafkaPublisher.createPublisher(KAFKA_BROKERS);
		receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
		// this.receiver.start(false, this); // In order to do the consume
		rep = new RepThreadsResources();
		impl = new JavaDirectory();	
		consume();
	}
	
	synchronized public static RepDirResources getInstance() {
		if (instance == null)
			instance = new RepDirResources();
		return instance;
	}
	
	private void sleep( int ms ) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void onReceive(ConsumerRecord<String, String> r) {
		// TODO Auto-generated method stub
		var version = r.offset();
		SyncPoint.getInstance().setVersion(version++);
		System.out.printf("Processing version of Consumer Record: (%d, ---->>>> ,%s)\n",version, r.value());

		var result = r.value();
		System.out.println("CONSUME REP DIR RESOURCES RESULT VALUE ---->>>> " + result);
		SyncPoint.getInstance().setResult( version, result);
		System.out.printf("SeqN: %s %d %s\n", r.topic(), r.offset(), r.value());
	}
	
	private void consume() {
		System.out.println("CHEGUEI CONSUME REP DIR RESOURCES ---->>>> ");
		receiver.start(true, (r) -> {
			onReceive(r);
		});
	}
	
	public void run(Long version) {
		String operation = "" + System.nanoTime();
		System.out.println("CHEGUEI WRITE FILE REP DIR RESOURCES OPERATION ---->>>> " + operation);
		
		version = sender.publish(TOPIC, operation);
		
		SyncPoint.getInstance().setVersion(version++);
		
		if (version >= 0)
			System.out.println("Message published with sequence number: " + version);
		else
			System.err.println("Failed to publish message");
		
		System.out.println("CHEGUEI WRITE FILE REP DIR RESOURCES VERSION ---->>>> " + version);
		var result = SyncPoint.getInstance().waitForResult(version);
		System.out.println("CHEGUEI WRITE FILE REP DIR RESOURCES RESULT ---->>>> " + result);
		System.out.printf("Op: %s, version: %s, result: %s\n", operation, version, result);
		sleep(500);
	}
	
	@Override
	public FileInfo writeFile(Long version, String filename, byte[] data, String userId, String password) {
		Log.info(String.format("REST writeFile: filename = %s, data.length = %d, userId = %s, password = %s \n",
				filename, data.length, userId, password));
		
		System.out.println("CHEGUEI WRITE FILE REP DIR RESOURCES ---->>>> ");
		
		run(version);
		// if(version > 0)
		// consume();
		
		// run();
		// SyncPoint.getInstance().setResult(++version, wr);
		// return super.resultOrThrowVersion(wr, version);
		return rep.writeFile(version, filename, data, userId, password); // to new class
	}

	@Override
	public void deleteFile(Long version, String filename, String userId, String password) {
		Log.info(String.format("REST deleteFile: filename = %s, userId = %s, password =%s\n", filename, userId,
				password));
		
		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version++);
		
		System.out.println("VERSION DELETE FILE DIRESOURCES --->> " + version);

		// super.resultOrThrowVersion(impl.deleteFile(filename, userId, password), version);
		rep.deleteFile(version, filename, userId, password);
	}

	@Override
	public void shareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Log.info(String.format("REST shareFile: filename = %s, userId = %s, userIdShare = %s, password =%s\n", filename,
				userId, userIdShare, password));
		
		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version++);
		
		System.out.println("VERSION SHARE FILE DIRESOURCES --->> " + version);

		// super.resultOrThrowVersion(impl.shareFile(filename, userId, userIdShare, password), version);
		rep.shareFile(version, filename, userId, userIdShare, password);
	}

	@Override
	public void unshareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Log.info(String.format("REST unshareFile: filename = %s, userId = %s, userIdShare = %s, password =%s\n",
				filename, userId, userIdShare, password));
		
		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version++);
		
		System.out.println("VERSION UNSHARE FILE DIRESOURCES --->> " + version);

		// super.resultOrThrowVersion(impl.unshareFile(filename, userId, userIdShare, password), version);
		rep.unshareFile(version, filename, userId, userIdShare, password);
	}

	@Override
	public byte[] getFile(Long version, String filename, String userId, String accUserId, String password) {
		Log.info(String.format("REST getFile: filename = %s, userId = %s, accUserId = %s, password =%s\n", filename,
				userId, accUserId, password));
		
		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version);
		
		System.out.println("VERSION GET FILE DIRESOURCES --->> " + version);
		
		// return super.resultOrThrowVersion(res, version);
		return rep.getFile(version, filename, userId, accUserId, password);
	}

	@Override
	public List<FileInfo> lsFile(Long version, String userId, String password) {
		Log.info(String.format("REST lsFile: userId = %s, password = %s\n", userId, password));
		
		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version);
		
		System.out.println("VERSION LS FILE DIRESOURCES --->> " + version);

		// return super.resultOrThrowVersion(impl.lsFile(userId, password), version);
		return rep.lsFile(version, userId, password);
	}

	@Override
	public void deleteUserFiles(Long version, String userId, String password, String token) {
		Log.info(String.format("REST deleteUserFiles: user = %s, password = %s, token = %s\n", userId, password, token));

		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version++);
		
		System.out.println("VERSION DELETE USER FILES DIRESOURCES --->> " + version);
		// super.resultOrThrow(impl.deleteUserFiles(userId, password, token));
		rep.deleteUserFiles(version, userId, password, token);
	}
}

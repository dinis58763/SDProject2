package tp2.impl.service.rest;

import static tp2.impl.clients.Clients.FilesClients;

import java.net.URI;
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
import tp2.api.service.java.Result.ErrorCode;
import tp2.api.service.rest.RepRestDirectory;
import tp2.impl.kafka.KafkaPublisher;
import tp2.impl.kafka.KafkaSubscriber;
import tp2.impl.kafka.KafkaUtils;
import tp2.impl.kafka.RecordProcessor;
import tp2.impl.service.common.JavaDirectory;
import tp2.impl.sync.SyncPoint;

@Singleton
public class RepDirResources extends Thread implements RepRestDirectory {
	// public class TotalOrderExecutor extends Thread implements RecordProcessor 
	// static final String FROM_BEGINNING = "earliest";
	// static final String TOPIC = "single_partition_topic";
	static final String TOPIC = "topic";
	// static final String KAFKA_BROKERS = "localhost:9092";
	static final String KAFKA_BROKERS = "kafka:9092";
	private static final String REST = "/rest/";
	
	// final RepThreadsResources rep;
	final KafkaPublisher sender;
	// final KafkaSubscriber receiver;
	final KafkaUtils kafkaUtils = new KafkaUtils();
	final RepDirectory impl;
	private static Logger Log = Logger.getLogger(RepDirResources.class.getName());
	private static RepDirResources instance;
	SyncPoint<String> sync;
	
	@SuppressWarnings("static-access")
	public RepDirResources() {
		kafkaUtils.createTopic(TOPIC,1,1);
		// consume();
		sender = KafkaPublisher.createPublisher(KAFKA_BROKERS);
		// receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
		// this.receiver.start(false, this); // In order to do the consume
		// rep = new RepThreadsResources();
		impl = new JavaDirectory();	
		sync = SyncPoint.getInstance();
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
/**
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
	***/
	
	@Override
	public FileInfo writeFile(Long version, String filename, byte[] data, String userId, String password) {
		Log.info(String.format("REST writeFile: filename = %s, data.length = %d, userId = %s, password = %s \n",
				filename, data.length, userId, password));
		
		System.out.println("CHEGUEI WRITE FILE REP DIR RESOURCES ---->>>> ");
		
		//run(version);
		String operation = "Writing File.... " + System.nanoTime();
		System.out.println("CHEGUEI WRITE FILE REP DIR RESOURCES OPERATION ---->>>> " + operation);
		
		version = sender.publish(TOPIC, operation);
		
		// sync.setVersion(version++); // Starts at 0
		
		if (version >= 0) {
			System.out.println("Message published with sequence number: " + version);
			// var result = sync.waitForResult(version);
			// System.out.println("CHEGUEI WRITE FILE REP DIR RESOURCES RESULT ---->>>> " + result);
			// System.out.printf("Op: %s, version: %s, result: %d\n", operation, version, result);
			sleep(500);
		}
		else
			System.err.println("Failed to publish message");
		
		return resultOrThrowVersion(impl.writeFile(version, filename, data, userId, password), version);
	}
	
	@Override
	public void deleteFile(Long version, String filename, String userId, String password) {
		Log.info(String.format("REST deleteFile: filename = %s, userId = %s, password =%s\n", filename, userId,
				password));
		
		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version++);
		
		System.out.println("VERSION DELETE FILE DIRESOURCES --->> " + version);

		resultOrThrowVersion(impl.deleteFile(version, filename, userId, password), version);
	}

	@Override
	public void shareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Log.info(String.format("REST shareFile: filename = %s, userId = %s, userIdShare = %s, password =%s\n", filename,
				userId, userIdShare, password));
		
		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version++);
		
		System.out.println("VERSION SHARE FILE DIRESOURCES --->> " + version);

		resultOrThrowVersion(impl.shareFile(version, filename, userId, userIdShare, password), version);;
	}

	@Override
	public void unshareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Log.info(String.format("REST unshareFile: filename = %s, userId = %s, userIdShare = %s, password =%s\n",
				filename, userId, userIdShare, password));
		
		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version++);
		
		System.out.println("VERSION UNSHARE FILE DIRESOURCES --->> " + version);

		resultOrThrowVersion(impl.unshareFile(version, filename, userId, userIdShare, password), version);
	}

	@Override
	public byte[] getFile(Long version, String filename, String userId, String accUserId, String password) {
		Log.info(String.format("REST getFile: filename = %s, userId = %s, accUserId = %s, password =%s\n", filename,
				userId, accUserId, password));
		
		// SyncPoint.getInstance().waitForResult(version);
		// SyncPoint.getInstance().setVersion(version);
		
		System.out.println("VERSION GET FILE DIRESOURCES --->> " + version);
		
		System.out.println("VERSION GET FILE REP DIR RESOURCES --->> " + version);
		
		var res = impl.getFile(version, filename, userId, accUserId, password);
		if (res.error() == ErrorCode.REDIRECT) {
			String location = res.errorValue();
			if (!location.contains(REST))
				res = FilesClients.get(location).getFile(JavaDirectory.fileId(filename, userId), password);
		}
		
		return resultOrThrowVersion(res, version);
	}

	@Override
	public List<FileInfo> lsFile(Long version, String userId, String password) {
		Log.info(String.format("REST lsFile: userId = %s, password = %s\n", userId, password));
		
		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version);
		
		System.out.println("VERSION LS FILE DIRESOURCES --->> " + version);

		return resultOrThrowVersion(impl.lsFile(version, userId, password), version);
	}

	@Override
	public void deleteUserFiles(Long version, String userId, String password, String token) {
		Log.info(String.format("REST deleteUserFiles: user = %s, password = %s, token = %s\n", userId, password, token));

		SyncPoint.getInstance().waitForResult(version);
		SyncPoint.getInstance().setVersion(version++);
		
		System.out.println("VERSION DELETE USER FILES DIRESOURCES --->> " + version);
		resultOrThrowVersion(impl.deleteUserFiles(version, userId, password, token), version);
	}
	
// ------------------------------------ PRIVATE METHODS ----------------------------------------------------------
	
	
	protected <T> T resultOrThrowVersion(Result<T> result, Long version) {
		System.out.println("VERSION REST RESOURCE --->>>> " + version);
		if (result.isOK()) {
			System.out.println("REST RESOURCE IF CORRECT VERSION --->>>> " + version);
			throw new WebApplicationException(Response.ok().header(RepRestDirectory.HEADER_VERSION, version)
					.entity(result.value()).build());
		}
		else
			throw new WebApplicationException(statusCode(result));
	}
	
	/**
	 * Translates a Result<T> to a HTTP Status code
	 */
	static protected Status statusCode(Result<?> result) {
		switch (result.error()) {
		case CONFLICT:
			return Status.CONFLICT;
		case NOT_FOUND:
			return Status.NOT_FOUND;
		case FORBIDDEN:
			return Status.FORBIDDEN;
		case TIMEOUT:
		case BAD_REQUEST:
			return Status.BAD_REQUEST;
		case NOT_IMPLEMENTED:
			return Status.NOT_IMPLEMENTED;
		case INTERNAL_ERROR:
			return Status.INTERNAL_SERVER_ERROR;
		case OK:
			return result.value() == null ? Status.NO_CONTENT : Status.OK;
		case REDIRECT:
			doRedirect(result);

		default:
			return Status.INTERNAL_SERVER_ERROR;
		}
	}

	static private void doRedirect(Result<?> result) throws WebApplicationException {
		var location = URI.create(result.errorValue());
		throw new WebApplicationException(Response.temporaryRedirect(location).build());
	}
}

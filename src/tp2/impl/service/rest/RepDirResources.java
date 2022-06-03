package tp2.impl.service.rest;

import static tp2.impl.clients.Clients.FilesClients;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.gson.Gson;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import tp2.api.FileInfo;
import tp2.api.service.java.Directory;
import tp2.api.service.java.Result;
import tp2.api.service.java.Result.ErrorCode;
import tp2.api.service.rest.RepRestDirectory;
import tp2.api.service.rest.RestDirectory;
import tp2.impl.kafka.KafkaPublisher;
import tp2.impl.kafka.KafkaSubscriber;
import tp2.impl.kafka.KafkaUtils;
import tp2.impl.kafka.RecordProcessor;
import tp2.impl.service.common.JavaDirectory;
import tp2.impl.sync.SyncPoint;
import tp2.replica.msgs.DeleteFileRep;
import tp2.replica.msgs.DeleteUserFiles;
import tp2.replica.msgs.ShareFileRep;
import tp2.replica.msgs.UnshareFileRep;
import tp2.replica.msgs.WriteFileRep;

@Singleton
public class RepDirResources<T> extends Thread implements RepRestDirectory, RecordProcessor {
	
	private static final String FROM_BEGINNING = "earliest";
	private static final String TOPIC = "topic";
	private static final String KAFKA_BROKERS = "kafka:9092";
	private static final String REST = "/rest/";
	
	private final KafkaUtils kafkaUtils = new KafkaUtils();
	private final KafkaPublisher sender;
	private final KafkaSubscriber receiver;
	private final Directory impl;
	private final SyncPoint<Result<T>> sync;
	// private Gson json;
	ObjectMapper objectMapper;
	private static Logger Log = Logger.getLogger(RepDirResources.class.getName());
	private static RepDirResources instance;
	
	@SuppressWarnings("static-access")
	public RepDirResources() {
		kafkaUtils.createTopic(TOPIC,1,1);
		sender = KafkaPublisher.createPublisher(KAFKA_BROKERS);
		receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
		impl = new JavaDirectory();	
		sync = SyncPoint.getInstance();
		objectMapper = new ObjectMapper();
		// json = new Gson();
		consume();
	}
	
	synchronized public static RepDirResources getInstance() {
		if (instance == null)
			instance = new RepDirResources();
		return instance;
	}
	
	@Override
	public FileInfo writeFile(Long version, String filename, byte[] data, String userId, String password) {
		Log.info(String.format("REST writeFile: filename = %s, data.length = %d, userId = %s, password = %s, version = %s \n",
				filename, data.length, userId, password, version));
		
		
		WriteFileRep wf = null;
		
		try {
			
			wf = new WriteFileRep("WF", filename, data, userId, password); 
			version = sender.publish(TOPIC, "WF-" + objectMapper.writeValueAsString(wf));
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (version >= 0)
			System.out.println("Message published with sequence number: " + version);
		else
			System.err.println("Failed to publish message");
		
		var result = sync.waitForResult(version);
		
		System.out.printf("Op Write: %s, version: %s, result: %s\n", wf, version, result);
		
		sleep(500);
		return (FileInfo) resultOrThrow(result);
	}
	
	@Override
	public void deleteFile(Long version, String filename, String userId, String password) {
		Log.info(String.format("REST deleteFile: filename = %s, userId = %s, password =%s, version = %s\n", filename, userId,
				password, version));
		
		DeleteFileRep wf = null;
		
		try {
			
			wf = new DeleteFileRep("DF", filename, userId, password); 
			version = sender.publish(TOPIC, "DF-" + objectMapper.writeValueAsString(wf));
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (version >= 0)
			System.out.println("Message published with sequence number: " + version);
		else
			System.err.println("Failed to publish message");
		
		var result = sync.waitForResult(version);
		
		System.out.printf("Op Delete: %s, version: %s, result: %s\n", wf, version, result);
		
		sleep(500);
		resultOrThrow(result);
	}

	@Override
	public void shareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Log.info(String.format("REST shareFile: filename = %s, userId = %s, userIdShare = %s, password =%s, "
				+ "version = %s\n", filename, userId, userIdShare, password, version));
		
		ShareFileRep wf = null;
		
		try {
			
			wf = new ShareFileRep("SF", filename, userId, userIdShare, password); 
			version = sender.publish(TOPIC, "SF-" + objectMapper.writeValueAsString(wf));
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (version >= 0)
			System.out.println("Message published with sequence number: " + version);
		else
			System.err.println("Failed to publish message");
		
		var result = sync.waitForResult(version);
		
		System.out.printf("Op Share: %s, version: %s, result: %s\n", wf, version, result);
		
		sleep(500);
		resultOrThrow(result);
	}

	@Override
	public void unshareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Log.info(String.format("REST unshareFile: filename = %s, userId = %s, userIdShare = %s, password =%s"
				+ "version = %s\n", filename, userId, userIdShare, password, version));
		
		UnshareFileRep wf = null;
		
		try {
			
			wf = new UnshareFileRep("UF", filename, userId, userIdShare, password); 
			version = sender.publish(TOPIC, "UF-" + objectMapper.writeValueAsString(wf));
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (version >= 0)
			System.out.println("Message published with sequence number: " + version);
		else
			System.err.println("Failed to publish message");
		
		var result = sync.waitForResult(version);
		
		System.out.printf("Op Unshare: %s, version: %s, result: %s\n", wf, version, result);
		
		sleep(500);
		resultOrThrow(result);
	}
	
	@Override
	public byte[] getFile(Long version, String filename, String userId, String accUserId, String password) {
		Log.info(String.format("REST getFile: filename = %s, userId = %s, accUserId = %s, password =%s, "
				+ "version = %s \n", filename, userId, accUserId, password, version));
		
		var res = impl.getFile(filename, userId, accUserId, password);
		System.out.println("GET FILE DIRESOURCES IMPL.GETFILE RES VALUE --->> " + res);
		
		if (res.error() == ErrorCode.REDIRECT) {
			String location = res.errorValue();
			if (!location.contains(REST))
				res = FilesClients.get(location).getFile(JavaDirectory.fileId(filename, userId), password);
		}
		
		// return resultOrThrowVersion(res, version);
		return resultOrThrow(res);
	}

	@Override
	public List<FileInfo> lsFile(Long version, String userId, String password) {
		Log.info(String.format("REST lsFile: userId = %s, password = %s, version = %s\n", userId, password, version));
		
		System.out.println("VERSION LS FILE DIRESOURCES --->> " + version);
		var ls = impl.lsFile(userId, password);

		return resultOrThrow(ls);
	}

	@Override
	public void deleteUserFiles(Long version, String userId, String password, String token) {
		Log.info(String.format("REST deleteUserFiles: user = %s, password = %s, token = %s"
				+ "version = %s \n", userId, password, token, version));
		
		System.out.println("VERSION DELETE USER FILES DIRESOURCES --->> " + version);
		DeleteUserFiles wf = null;
		
		try {
			
			wf = new DeleteUserFiles(userId, password, token); 
			version = sender.publish(TOPIC, "DUF-" + objectMapper.writeValueAsString(wf));
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (version >= 0)
			System.out.println("Message published with sequence number: " + version);
		else
			System.err.println("Failed to publish message");
		
		var result = sync.waitForResult(version);
		
		System.out.printf("Op Delete User Files: %s, version: %s, result: %s\n", wf, version, result);
		
		sleep(500);
		resultOrThrow(result);
	}
	
	
	// ---------------------------   PRIVATE METHODS --------------------------------------------------------------
	
	// PUBLISH SENDER (SETS VERSION)
	private Long runVersion(String operation, Long version) {
		
		operation = operation + "-" + System.nanoTime();
		version = sender.publish(TOPIC, operation);
		
		if (version >= 0)
			System.out.println("Message published with sequence number: " + version);
		else
			System.err.println("Failed to publish message");
		
		var result = sync.waitForResult(version);
		
		System.out.printf("Op: %s, version: %s, result: %s\n", operation, version, result);
		// sleep(500);
		
		return version;
	}
	
	// THREAD SLEEP
	private void sleep( int ms ) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	// SUBSCRIBER RECEIVER 
	@SuppressWarnings("unchecked")
	private <T> void consume() {
		receiver.start(false, (r) -> { // new Thread(() -> consume(processor)).start();
			onReceive(r);
		});
	}  
	
	// ON RECEIVE (UPDATES VERSION)
	@SuppressWarnings("unchecked")
	@Override
	public void onReceive(ConsumerRecord<String, String> r) {
		// TODO Auto-generated method stub
		var version = r.offset();
		System.out.printf("Processing version of Consumer Record: (Version: %d, ---->>>> , Value: %s)\n",version, r.value());
		
		// var op = json.fromJson(r.value(), WriteFileRep.class);
		// objectMapper
		
		try {
			String[] s = r.value().split("-");
			System.out.println("VALUE OF THE STRING ARRAY ----->>>>  " + s);
			String op = s[0];
			System.out.println("VALUE OF THE FIRST PART OF THE STRING -------->>>> " + op);
			String op2 = s[1];
			System.out.println("VALUE OF THE SECOND PART OF THE STRING -------->>>> " + op2);
			Result<T> result = null;
			
			switch(op) {
			case "WF":
				WriteFileRep wf = objectMapper.readValue(op2, WriteFileRep.class);
				result = (Result<T>) impl.writeFile(wf.filename(), wf.data(), wf.userId(), wf.password());
				System.out.println("Message consumed with result: " + result);
				break;
			case "DF":
				//  new DeleteFileRep("DF", filename, userId, password); 
				DeleteFileRep df = objectMapper.readValue(op2, DeleteFileRep.class);
				result = (Result<T>) impl.deleteFile(df.filename(), df.userId(), df.password());
				System.out.println("Message consumed with result: " + result);
				break;
			case "SF":
				// new ShareFileRep("SF", filename, userId, userIdShare, password);
				ShareFileRep sf = objectMapper.readValue(op2, ShareFileRep.class);
				result = (Result<T>) impl.shareFile(sf.filename(), sf.userId(), sf.userIdShare(), sf.password());
				System.out.println("Message consumed with result: " + result);
				break;
			case "UF":
				// new UnshareFileRep("UF", filename, userId, userIdShare, password); 
				UnshareFileRep uf = objectMapper.readValue(op2, UnshareFileRep.class);
				result = (Result<T>) impl.unshareFile(uf.filename(), uf.userId(), uf.userIdShare(), uf.password());
				System.out.println("Message consumed with result: " + result);
				break;
			case "DUF":
				// wf = new DeleteUserFiles(userId, password, token); 
				DeleteUserFiles duf = objectMapper.readValue(op2, DeleteUserFiles.class);
				result = (Result<T>) impl.deleteUserFiles(duf.userId(), duf.password(), duf.token());
				System.out.println("Message consumed with result: " + result);
				break;
			}
			
			sync.setResult( version, result);
			// var result = r.value();
			
			System.out.printf("SeqN Consumed: Topic: %s, Version: %d, Value: %s\n", r.topic(), r.offset(), r.value());
		
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
	
	protected <T> T resultOrThrow(Result<T> result) {
		if (result.isOK())
			return result.value();
		else
			throw new WebApplicationException(statusCode(result));
	}
/**	
	protected <T> T resultOrThrowVersion(Result<T> result, Long version) {
		System.out.println("VERSION REST RESOURCE --->>>> " + version);
		if (result.isOK()) {
			throw new WebApplicationException(Response.ok().header(RepRestDirectory.HEADER_VERSION, version)
					.entity(result.value()).build());
		}
		else
			throw new WebApplicationException(statusCode(result));
	}
**/	
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

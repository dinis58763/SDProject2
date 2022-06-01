package tp2.impl.service.rest;

import static tp2.impl.clients.Clients.FilesClients;

import java.util.List;
import java.util.logging.Logger;

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
import tp2.impl.service.common.JavaDirectory;
import tp2.impl.sync.SyncPoint;

@Singleton
public class RepThreadsResources extends RestResource implements RepRestDirectory {
	
	private static Logger Log = Logger.getLogger(RepThreadsResources.class.getName());

	private static final String REST = "/rest/";
	static final String TOPIC_TOTAL = "single_partition_topic";
	private static final String KAFKA_BROKERS = "kafka:9092"; // When running in docker container...

	final RepDirectory impl;

	public RepThreadsResources() {
		impl = new JavaDirectory();	
	}
	
	public FileInfo writeFile(Long version, String filename, byte[] data, String userId, String password) {
		Log.info(String.format("REST writeFile: filename = %s, data.length = %d, userId = %s, password = %s , "
				+ "version = %s \n",
				filename, data.length, userId, password, version));
	/**	
		KafkaConsumer<Integer, String> consumer;
		ConsumerRecords<String, String> records = consumer.poll(1000);
		records.forEach(r -> {
			// Process operation
			System.err.println(r.offset() + "->" + r.topic() + "/" + r.value());	
		});		
	**/	
		System.out.println("REP THREADS RESOURCES VERSION WRITE FILE --->> " + version);
		var wr = impl.writeFile(version, filename, data, userId, password);
		System.out.println("REP THREADS RESOURCES VERSION WRITE FILE WR VALUE --->> " + wr);
		// run();
		// SyncPoint.getInstance().setResult(++version, wr);
		return super.resultOrThrowVersion(wr, version);
	}

	@Override
	public void deleteFile(Long version, String filename, String userId, String password) {
		Log.info(String.format("REST deleteFile: filename = %s, userId = %s, password =%s\n", filename, userId,
				password));
		
		System.out.println("VERSION DELETE FILE REP DIR RESOURCES --->> " + version);
		
		var del = impl.deleteFile(version, filename, userId, password);
		super.resultOrThrowVersion(del, version);
	}

	@Override
	public void shareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Log.info(String.format("REST shareFile: filename = %s, userId = %s, userIdShare = %s, password =%s\n", filename,
				userId, userIdShare, password));
		
		System.out.println("VERSION SHARE FILE REP DIR RESOURCES --->> " + version);

		var sh = impl.shareFile(version, filename, userId, userIdShare, password);
		super.resultOrThrowVersion(sh, version);
	}

	@Override
	public void unshareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Log.info(String.format("REST unshareFile: filename = %s, userId = %s, userIdShare = %s, password =%s\n",
				filename, userId, userIdShare, password));
		
		System.out.println("VERSION UNSHARE FILE REP DIR RESOURCES --->> " + version);

		var unsh = impl.unshareFile(version, filename, userId, userIdShare, password);
		// SyncPoint.getInstance().setResult(++version, unsh);
		super.resultOrThrowVersion(unsh, version);
	}

	@Override
	public byte[] getFile(Long version, String filename, String userId, String accUserId, String password) {
		Log.info(String.format("REST getFile: filename = %s, userId = %s, accUserId = %s, password =%s\n", filename,
				userId, accUserId, password));

		System.out.println("VERSION GET FILE REP DIR RESOURCES --->> " + version);
		
		var res = impl.getFile(version, filename, userId, accUserId, password);
		if (res.error() == ErrorCode.REDIRECT) {
			String location = res.errorValue();
			if (!location.contains(REST))
				res = FilesClients.get(location).getFile(JavaDirectory.fileId(filename, userId), password);
		}
		return super.resultOrThrowVersion(res, version);
		// return super.resultOrThrow(res);
	}

	@Override
	public List<FileInfo> lsFile(Long version, String userId, String password) {
		long T0 = System.currentTimeMillis();
		try {

			Log.info(String.format("REST lsFile: userId = %s, password = %s\n", userId, password));
			
			System.out.println("VERSION LS FILE REP DIR RESOURCES --->> " + version);
			
			var ls = impl.lsFile(version, userId, password);
			//SyncPoint.getInstance().setResult(version, ls);
			return super.resultOrThrowVersion(ls, version);
			// return super.resultOrThrow(ls);
		} 
		
		finally {
			System.err.println("TOOK:" + (System.currentTimeMillis() - T0));
		}
	}

	@Override
	public void deleteUserFiles(Long version, String userId, String password, String token) {
		Log.info(
				String.format("REST deleteUserFiles: user = %s, password = %s, token = %s\n", userId, password, token));

		System.out.println("VERSION DELETE USER FILE REP DIR RESOURCES --->> " + version);
		
		var del = impl.deleteUserFiles(version, userId, password, token);
		// SyncPoint.getInstance().setResult(++version, del);
		super.resultOrThrowVersion(del, version);
	}
}

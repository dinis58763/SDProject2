package tp2.impl.service.rest;

import static tp2.impl.clients.Clients.FilesClients;

import java.util.List;
import java.util.logging.Logger;

import jakarta.inject.Singleton;
import tp2.api.FileInfo;
import tp2.api.service.java.Directory;
import tp2.api.service.java.Result.ErrorCode;
import tp2.api.service.rest.RestDirectory;
import tp2.impl.service.common.JavaDirectory;
import tp2.impl.sync.SyncPoint;

@Singleton
public class RepDirResources extends RestResource implements RestDirectory {
	private static Logger Log = Logger.getLogger(DirectoryResources.class.getName());

	private static final String REST = "/rest/";

	final Directory impl;

	public RepDirResources() {
		impl = new JavaDirectory();
	}

	public FileInfo writeFile(Long version, String filename, byte[] data, String userId, String password) {
		Log.info(String.format("REST writeFile: filename = %s, data.length = %d, userId = %s, password = %s , "
				+ "version = %s \n",
				filename, data.length, userId, password, version));
		
		var wr = impl.writeFile(filename, data, userId, password);
		SyncPoint.getInstance().setResult(++version, wr);
		
		return super.resultOrThrowVersion(wr, version);
	}

	@Override
	public void deleteFile(Long version, String filename, String userId, String password) {
		Log.info(String.format("REST deleteFile: filename = %s, userId = %s, password =%s\n", filename, userId,
				password));
		
		var del = impl.deleteFile(filename, userId, password);
		SyncPoint.getInstance().setResult(++version, del);
		
		super.resultOrThrowVersion(del, version);
	}

	@Override
	public void shareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Log.info(String.format("REST shareFile: filename = %s, userId = %s, userIdShare = %s, password =%s\n", filename,
				userId, userIdShare, password));

		var sh = impl.shareFile(filename, userId, userIdShare, password);
		SyncPoint.getInstance().setResult(++version, sh);
		
		super.resultOrThrowVersion(sh, version);
	}

	@Override
	public void unshareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Log.info(String.format("REST unshareFile: filename = %s, userId = %s, userIdShare = %s, password =%s\n",
				filename, userId, userIdShare, password));

		var unsh = impl.unshareFile(filename, userId, userIdShare, password);
		SyncPoint.getInstance().setResult(++version, unsh);
		
		super.resultOrThrowVersion(unsh, version);
	}

	@Override
	public byte[] getFile(Long version, String filename, String userId, String accUserId, String password) {
		Log.info(String.format("REST getFile: filename = %s, userId = %s, accUserId = %s, password =%s\n", filename,
				userId, accUserId, password));

		var res = impl.getFile(filename, userId, accUserId, password);
		if (res.error() == ErrorCode.REDIRECT) {
			String location = res.errorValue();
			if (!location.contains(REST))
				res = FilesClients.get(location).getFile(JavaDirectory.fileId(filename, userId), password);
		}
		SyncPoint.getInstance().setResult(version, res);
		
		return super.resultOrThrowVersion(res, version);
	}

	@Override
	public List<FileInfo> lsFile(Long version, String userId, String password) {
		long T0 = System.currentTimeMillis();
		try {

			Log.info(String.format("REST lsFile: userId = %s, password = %s\n", userId, password));
			
			var ls = impl.lsFile(userId, password);
			SyncPoint.getInstance().setResult(version, ls);
			
			return super.resultOrThrowVersion(ls, version);
		} 
		
		finally {
			System.err.println("TOOK:" + (System.currentTimeMillis() - T0));
		}
	}

	@Override
	public void deleteUserFiles(Long version, String userId, String password, String token) {
		Log.info(
				String.format("REST deleteUserFiles: user = %s, password = %s, token = %s\n", userId, password, token));

		var del = impl.deleteUserFiles(userId, password, token);
		SyncPoint.getInstance().setResult(++version, del);
		
		super.resultOrThrowVersion(del, version);
	}
}

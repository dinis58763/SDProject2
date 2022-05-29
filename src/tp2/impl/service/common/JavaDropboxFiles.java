package tp2.impl.service.common;

import static tp2.api.service.java.Result.error;
import static tp2.api.service.java.Result.ok;
import static tp2.api.service.java.Result.ErrorCode.INTERNAL_ERROR;
import static tp2.api.service.java.Result.ErrorCode.NOT_FOUND;

import tp2.api.service.java.Files;
import tp2.api.service.java.Result;
import tp2.dropbox.CreateDirectory;
import tp2.dropbox.CreateFile;
import tp2.dropbox.DeleteFile;
import tp2.dropbox.GetFile;


public class JavaDropboxFiles implements Files {

	static final String DELIMITER = "$$$";
	private static final String ROOT = "/tmp";
	
	public JavaDropboxFiles() {
		try {
			new CreateDirectory().execute(ROOT);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Result<byte[]> getFile(String fileId, String token) {
		fileId = fileId.replace( DELIMITER, "/");
		byte[] data = null;
		try {
			data = new GetFile().execute(ROOT + "/" + fileId);
			System.out.println(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data != null ? ok( data) : error( NOT_FOUND );
	}

	@Override
	public Result<Void> deleteFile(String fileId, String token) {
		fileId = fileId.replace( DELIMITER, "/");
		boolean res = false;
		try {
			res = new DeleteFile().execute(ROOT + "/" + fileId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res ? ok() : error( NOT_FOUND );
	}

	@Override
	public Result<Void> writeFile(String fileId, byte[] data, String token) {
		fileId = fileId.replace( DELIMITER, "/");
		try {
			new CreateFile().execute(ROOT + "/" + fileId, data, token);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ok();
	}

	@Override
	public Result<Void> deleteUserFiles(String userId, String token) {
		try {
			new DeleteFile().execute(ROOT + "/" + userId);
		} catch (Exception e) {
			e.printStackTrace();
			return error(INTERNAL_ERROR);
		}
		return ok();
	}

}

package tp2.dropbox;

import org.pac4j.scribe.builder.api.DropboxApi20;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;

import tp2.dropbox.msgs.DeleteFileV1;

public class DeleteFile {

	private static final String apiKey = "9fo2s3ygnwya7y5";
	private static final String apiSecret = "t9lzgqfptwvuvb0";
	private static final String accessTokenStr = "sl.BIiz6VZH103yyiyiXO3M1t97vjucGRxG1aYjQmMvfW7uymY6nLQmZGlDx3KBAFvClFsizy_IyTi1tLpxjSu3N3K2JA-RbOfhYAWVuSo_Qt8-ST2fxjf32vSH2kRd67WyyqppEwEg";
	
	private static final String DELETE_FILE_URL = "https://api.dropboxapi.com/2/files/delete_v2";
	
	private static final int HTTP_SUCCESS = 200;
	private static final int HTTP_CONFLICT = 409;
	private static final String CONTENT_TYPE_HDR = "Content-Type";
	private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
	
	private final Gson json;
	private final OAuth20Service service;
	private final OAuth2AccessToken accessToken;
		
	public DeleteFile() {
		json = new Gson();
		accessToken = new OAuth2AccessToken(accessTokenStr);
		service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
	}
	
	public boolean execute( String path ) throws Exception {
		
		var deleteFile = new OAuthRequest(Verb.POST, DELETE_FILE_URL);
		deleteFile.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
		
		deleteFile.setPayload(json.toJson(new DeleteFileV1(path)));

		service.signRequest(accessToken, deleteFile);
		
		Response r = service.execute(deleteFile);
		if (r.getCode() != HTTP_SUCCESS && r.getCode() != HTTP_CONFLICT) 
			throw new RuntimeException(String.format("Failed to delete file on path: %s, Status: %d, \nReason: %s\n", path, r.getCode(), r.getBody()));
		return true;
	}
	
	public static void main(String[] args) throws Exception {

		if( args.length != 1 ) {
			System.err.println("usage: java DeleteDirectory <path>");
			System.exit(0);
		}
		
		var path = args[0];
		var cd = new DeleteFile();
		
		cd.execute(path);
		System.out.println("File on path '" + path + "' deleted successfuly.");
	}

}

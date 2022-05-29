package tp2.dropbox;

import org.pac4j.scribe.builder.api.DropboxApi20;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;

import tp2.dropbox.msgs.CreateFileV1;

public class CreateFile {

	private static final String apiKey = "9fo2s3ygnwya7y5";
	private static final String apiSecret = "t9lzgqfptwvuvb0";
	private static final String accessTokenStr = "sl.BIiz6VZH103yyiyiXO3M1t97vjucGRxG1aYjQmMvfW7uymY6nLQmZGlDx3KBAFvClFsizy_IyTi1tLpxjSu3N3K2JA-RbOfhYAWVuSo_Qt8-ST2fxjf32vSH2kRd67WyyqppEwEg";
	
	private static final String CREATE_FILE_URL = "https://content.dropboxapi.com/2/files/upload";
	
	private static final int HTTP_SUCCESS = 200;
	private static final String CONTENT_TYPE_HDR = "Content-Type";
	private static final String OCTET_CONTENT_TYPE = "application/octet-stream";
	private static final String DROPBOX_API_ARG = "Dropbox-API-Arg";
	
	private final Gson json;
	private final OAuth20Service service;
	private final OAuth2AccessToken accessToken;
		
	public CreateFile() {
		json = new Gson();
		accessToken = new OAuth2AccessToken(accessTokenStr);
		service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
	}
	
	public void execute( String path, byte[] data, String token ) throws Exception {
		
		CreateFileV1 file = new CreateFileV1(path, "overwrite" , false, false, false);
		
		var createFile = new OAuthRequest(Verb.POST, CREATE_FILE_URL);
		createFile.addHeader(CONTENT_TYPE_HDR, OCTET_CONTENT_TYPE);
		createFile.addHeader(DROPBOX_API_ARG, json.toJson(file));

		createFile.setPayload(data);

		service.signRequest(accessToken, createFile);
		
		Response r = service.execute(createFile);
		if (r.getCode() != HTTP_SUCCESS) 
			throw new RuntimeException(String.format("Failed to create file: %s, Status: %d, \nReason: %s\n", path, r.getCode(), r.getBody()));
	}
	
	public static void main(String[] args) throws Exception {

		if( args.length != 3 ) {
			System.err.println("usage: java CreateFile <filename> <path> <filedata>");
			System.exit(0);
		}
		
		var path = args[0];
		byte[] data = args[1].getBytes();
		String token = args[2];
		var cd = new CreateFile();
		
		cd.execute(path, data, token);
		System.out.println("File '" + path + "' created successfuly.");
	}

}

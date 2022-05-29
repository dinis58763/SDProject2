package tp2.dropbox;

import org.pac4j.scribe.builder.api.DropboxApi20;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;

import tp2.dropbox.msgs.GetFileV1;

public class GetFile {

	private static final String apiKey = "9fo2s3ygnwya7y5";
	private static final String apiSecret = "t9lzgqfptwvuvb0";
	private static final String accessTokenStr = "sl.BIiz6VZH103yyiyiXO3M1t97vjucGRxG1aYjQmMvfW7uymY6nLQmZGlDx3KBAFvClFsizy_IyTi1tLpxjSu3N3K2JA-RbOfhYAWVuSo_Qt8-ST2fxjf32vSH2kRd67WyyqppEwEg";
	
	private static final String GET_FILE_URL = "https://content.dropboxapi.com/2/files/download";
	
	private static final int HTTP_SUCCESS = 200;
	private static final String CONTENT_TYPE_HDR = "Content-Type";
	private static final String OCTET_CONTENT_TYPE = "application/octet-stream";
	private static final String DROPBOX_API_ARG = "Dropbox-API-Arg";
	
	private final Gson json;
	private final OAuth20Service service;
	private final OAuth2AccessToken accessToken;
		
	public GetFile() {
		json = new Gson();
		accessToken = new OAuth2AccessToken(accessTokenStr);
		service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
	}
	
	public byte[] execute(String path) throws Exception {
		
		GetFileV1 file = new GetFileV1(path);
		
		var getFile = new OAuthRequest(Verb.POST, GET_FILE_URL);
		getFile.addHeader(CONTENT_TYPE_HDR, OCTET_CONTENT_TYPE);
		getFile.addHeader(DROPBOX_API_ARG, json.toJson(file));
		
		//getFile.setPayload(json.toJson(new GetFileV1(path)));

		service.signRequest(accessToken, getFile);
		
		Response r = service.execute(getFile);
		if (r.getCode() != HTTP_SUCCESS) 
			throw new RuntimeException(String.format("Failed to download from: %s, Status: %d, \nReason: %s\n", path,  r.getCode(), r.getBody()));
		var in = r.getStream();
		return in.readAllBytes();
	}
	
	public static void main(String[] args) throws Exception {

		if( args.length != 1 ) {
			System.err.println("usage: java GetFile <path>");
			System.exit(0);
		}
		
		var path = args[0];
		var cd = new GetFile();

		cd.execute(path);
		System.out.println("Downloaded successfully from " + path);
	}

}

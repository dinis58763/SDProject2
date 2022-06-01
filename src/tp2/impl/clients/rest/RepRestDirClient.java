package tp2.impl.clients.rest;

import java.net.URI;
import java.util.List;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import tp2.api.FileInfo;
import tp2.api.service.java.RepDirectory;
import tp2.api.service.java.Result;
import tp2.api.service.rest.RepRestDirectory;

public class RepRestDirClient extends RestClient implements RepDirectory {

	private static final String SHARE = "share";
	
	public RepRestDirClient(URI serverUri) {
		super(serverUri, RepRestDirectory.PATH);
	}

	@Override
	public Result<FileInfo> writeFile(Long version, String filename, byte[] data, String userId, String password) {
		System.err.println(target);
		Response r = target.path(userId)
				.path(filename)
				.queryParam(RepRestDirectory.PASSWORD, password)
				.request()
				.header(RepRestDirectory.HEADER_VERSION, version) //.header(RepRestDirectory.HEADER_VERSION, version)
				.accept(MediaType.APPLICATION_JSON)
				.post(Entity.entity( data, MediaType.APPLICATION_OCTET_STREAM));
		return super.responseContents(r, Status.OK, new GenericType<FileInfo>() {});
	}

	@Override
	public Result<Void> deleteFile(Long version, String filename, String userId, String password) {
		Response r = target.path(userId)
				.path(filename)
				.queryParam(RepRestDirectory.PASSWORD, password)
				.request()
				.header(RepRestDirectory.HEADER_VERSION, version) //.header(RepRestDirectory.HEADER_VERSION, 1)
				.delete();
		return super.verifyResponse(r, Status.NO_CONTENT);
	}

	@Override
	public Result<Void> shareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Response r = target.path(userId)
				.path(filename)
				.path(SHARE)
				.path( userIdShare)
				.queryParam(RepRestDirectory.PASSWORD, password)
				.request()
				.header(RepRestDirectory.HEADER_VERSION, version) // .header(RepRestDirectory.HEADER_VERSION, 1)
				.post(Entity.json(null));		
		return super.verifyResponse(r, Status.NO_CONTENT);
	}

	@Override
	public Result<Void> unshareFile(Long version, String filename, String userId, String userIdShare, String password) {
		Response r = target.path(userId)
				.path(filename)
				.path(SHARE)
				.path( userIdShare)
				.queryParam(RepRestDirectory.PASSWORD, password)
				.request()
				.header(RepRestDirectory.HEADER_VERSION, version) //.header(RepRestDirectory.HEADER_VERSION, 1)
				.delete();
		return super.verifyResponse(r, Status.NO_CONTENT);
	}

	@Override
	public Result<byte[]> getFile(Long version, String filename, String userId, String accUserId, String password) {
		Response r = target.path(userId)
				.path(filename)
				.queryParam(RepRestDirectory.ACC_USER_ID, accUserId)
				.queryParam(RepRestDirectory.PASSWORD, password)
				.request()
				.header(RepRestDirectory.HEADER_VERSION, version) //.header(RepRestDirectory.HEADER_VERSION, 1)
				.accept(MediaType.APPLICATION_OCTET_STREAM)
				.get();
		return super.responseContents(r, Status.OK, new GenericType<byte[]>() {});
	}

	@Override
	public Result<List<FileInfo>> lsFile(Long version, String userId, String password) {
		Response r = target.path(userId)
				.queryParam(RepRestDirectory.PASSWORD, password)
				.request()
				.header(RepRestDirectory.HEADER_VERSION, version) //.header(RepRestDirectory.HEADER_VERSION, 1)
				.accept(MediaType.APPLICATION_JSON)
				.get();
		return super.responseContents(r, Status.OK, new GenericType<List<FileInfo>>() {});
	}

	@Override
	public Result<Void> deleteUserFiles(Long version, String userId, String password, String token) {
		Response r = target.path(userId)
				.queryParam(RepRestDirectory.PASSWORD, password)
				.queryParam(RepRestDirectory.TOKEN, token)
				.request()
				.header(RepRestDirectory.HEADER_VERSION, version) //.header(RepRestDirectory.HEADER_VERSION, 1)
				.delete();
		return super.verifyResponse(r, Status.NO_CONTENT);
	}
}

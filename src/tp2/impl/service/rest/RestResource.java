package tp2.impl.service.rest;

import java.net.URI;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import tp2.api.service.rest.RepRestDirectory;
import tp2.api.service.rest.RestDirectory;
import tp2.api.service.java.Result;

public class RestResource {

	/**
	 * Given a Result<T>, either returns the value, or throws the JAX-WS Exception
	 * matching the error code...
	 */
	protected <T> T resultOrThrow(Result<T> result) {
		if (result.isOK())
			return result.value();
		else
			throw new WebApplicationException(statusCode(result));
	}
	
	protected <T> T resultOrThrowVersion(Result<T> result, Long version) {
		System.out.println("VERSION REST RESOURCE --->>>> " + version);
		if (result.isOK()) {
			System.out.println("REST RESOURCE IF CORRECT VERSION --->>>> " + version);
			throw new WebApplicationException(Response.ok(result.value()).header(RepRestDirectory.HEADER_VERSION, version)
					.entity(result.value()).build());
			// return result.value()
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
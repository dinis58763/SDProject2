package tp2.impl.clients.soap;

import static tp2.api.service.java.Result.error;
import static tp2.api.service.java.Result.ok;

import java.net.URI;

import javax.net.ssl.HttpsURLConnection;

import com.sun.xml.ws.client.BindingProviderProperties;

import jakarta.xml.ws.BindingProvider;
import jakarta.xml.ws.WebServiceException;
import tp2.api.service.java.Result;
import tp2.api.service.java.Result.ErrorCode;
import tp2.impl.clients.common.RetryClient;
import tp2.tls.InsecureHostnameVerifier;

/**
* 
* Shared behavior among SOAP clients.
* 
* Holds endpoint information.
* 
* Translates soap responses/exceptions to Result<T> for interoperability.
*  
* @author smduarte
*
*/
abstract class SoapClient extends RetryClient {
	
	protected static final String WSDL = "?wsdl";

	protected final URI uri;

	public SoapClient(URI uri) {
		//This allows client code executed by this process to ignore hostname verification
		HttpsURLConnection.setDefaultHostnameVerifier(new InsecureHostnameVerifier());
		
		this.uri = uri;
	}

	static interface ResultSupplier<T> {
		T get() throws Exception;
	}

	static interface VoidSupplier {
		void run() throws Exception;
	}

	protected <T> Result<T> tryCatchResult(ResultSupplier<T> supplier) {
		try {
			return ok( supplier.get());	
		} 
		catch (Exception e) {			
			if( e instanceof WebServiceException ) {
//				e.printStackTrace();
				throw new RuntimeException( e.getMessage() );
			}			
			return error(getErrorCodeFrom(e));
		}
	}

	protected <T> Result<T> tryCatchVoid( VoidSupplier r) {
		try {
			r.run();
			return ok();
		}
		catch (Exception e) {
			if( e instanceof WebServiceException ) {
//				e.printStackTrace();
				throw new RuntimeException( e.getMessage() );				
			}
			return error(getErrorCodeFrom(e));
		}
	}

	@Override
	public String toString() {
		return uri.toString();
	}

	static private ErrorCode getErrorCodeFrom(Exception e) {
		return switch (e.getMessage()) {
			case "OK" -> ErrorCode.OK;
			case "CONFLICT" -> ErrorCode.CONFLICT;
			case "NOT_FOUND" -> ErrorCode.NOT_FOUND;
			case "FORBIDDEN" -> ErrorCode.FORBIDDEN;
			case "INTERNAL_ERROR" -> ErrorCode.INTERNAL_ERROR;
			case "NOT_IMPLEMENTED" -> ErrorCode.NOT_IMPLEMENTED;
			default -> ErrorCode.INTERNAL_ERROR;
		};
	}

	void setTimeouts(BindingProvider port ) {
		port.getRequestContext().put(BindingProviderProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
		port.getRequestContext().put(BindingProviderProperties.REQUEST_TIMEOUT, READ_TIMEOUT);		
	}

}

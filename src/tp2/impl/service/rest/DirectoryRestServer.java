package tp2.impl.service.rest;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tp2.api.service.java.Directory;
import tp2.impl.service.rest.util.CustomLoggingFilter;
import tp2.impl.service.rest.util.GenericExceptionMapper;
import util.Token;

public class DirectoryRestServer extends AbstractRestServer {
	public static final int PORT = 4567;
	
	private static Logger Log = Logger.getLogger(DirectoryRestServer.class.getName());

	DirectoryRestServer( int port ) {
		super(Log, Directory.SERVICE_NAME, port);
	}
	
	@Override
	void registerResources(ResourceConfig config) {
		config.register( DirectoryResources.class ); 
		config.register( GenericExceptionMapper.class );
		
		config.register( CustomLoggingFilter.class);
	}
	
	public static void main(String[] args) throws Exception {

		Token.set( args.length > 0 ? args[0] : "");

		new DirectoryRestServer(PORT).start();
	}	
}
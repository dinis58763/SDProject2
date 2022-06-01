package tp2.impl.service.rest;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tp2.api.service.java.Files;
import tp2.impl.service.rest.util.GenericExceptionMapper;
import util.Token;

public class FilesRestServer extends AbstractRestServer {
	public static final int PORT = 5678;
	
	private static Logger Log = Logger.getLogger(FilesRestServer.class.getName());

	
	FilesRestServer( int port ) {
		super(Log, Files.SERVICE_NAME, port);
	}
	
	@Override
	void registerResources(ResourceConfig config) {
		config.register( FilesResources.class ); 
		config.register( GenericExceptionMapper.class );
//		config.register( CustomLoggingFilter.class);
	}
	
	public static void main(String[] args) throws Exception {
		
		Token.set( args.length == 0 ? "" : args[0] );

		new FilesRestServer(PORT).start();
	}	
}
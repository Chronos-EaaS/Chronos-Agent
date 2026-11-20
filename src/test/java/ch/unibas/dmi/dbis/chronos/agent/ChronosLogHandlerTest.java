package ch.unibas.dmi.dbis.chronos.agent;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.core.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class ChronosLogHandlerTest {

    private final CountDownLatch uploadStarted = new CountDownLatch( 1 );
    private final CountDownLatch allowResponse = new CountDownLatch( 1 );
    private final AtomicInteger uploads = new AtomicInteger();
    private final ExecutorService callers = Executors.newFixedThreadPool( 2 );
    private HttpServer server;
    private ChronosHttpClient.ChronosLogHandler handler;


    @Before
    public void startServer() throws IOException {
        InetAddress address = InetAddress.getByName( "127.0.0.1" );
        server = HttpServer.create( new InetSocketAddress( address, 0 ), 0 );
        server.createContext( "/api/v1/job", this::handleUpload );
        server.start();

        ChronosHttpClient client = new ChronosHttpClient( address, server.getAddress().getPort(), false, false );
        JSONObject job = new JSONObject()
                .put( "id", 1 ).put( "user", 1 ).put( "status", 2 )
                .put( "username", "test" ).put( "name", "test" ).put( "description", "" )
                .put( "system", "test" ).put( "cdl", "" ).put( "created", "" );
        handler = client.new ChronosLogHandler( new ChronosJob( job ) );
    }


    private void handleUpload( HttpExchange exchange ) throws IOException {
        try {
            exchange.getRequestBody().readAllBytes();
            uploads.incrementAndGet();
            uploadStarted.countDown();
            allowResponse.await();
            byte[] response = "{\"status\":{\"code\":200}}".getBytes( StandardCharsets.UTF_8 );
            exchange.sendResponseHeaders( 200, response.length );
            exchange.getResponseBody().write( response );
        } catch ( InterruptedException ex ) {
            Thread.currentThread().interrupt();
        } finally {
            exchange.close();
        }
    }


    @After
    public void stopServer() throws InterruptedException {
        allowResponse.countDown();
        callers.shutdownNow();
        try {
            if ( handler != null ) {
                handler.close();
            }
        } finally {
            if ( server != null ) {
                server.stop( 0 );
            }
        }
    }


    @Test
    public void concurrentFlushesBothWaitForThePendingUpload() throws Exception {
        handler.publish( "message" );
        assertTrue( uploadStarted.await( 5, SECONDS ) );

        CountDownLatch flushersReady = new CountDownLatch( 2 );
        CompletionService<Void> completions = new ExecutorCompletionService<>( callers );
        for ( int i = 0; i < 2; i++ ) {
            completions.submit( () -> {
                flushersReady.countDown();
                flushersReady.await();
                handler.flush();
                return null;
            } );
        }
        assertTrue( flushersReady.await( 5, SECONDS ) );
        assertNull( "A flush returned before the pending upload completed", completions.poll( 250, MILLISECONDS ) );

        allowResponse.countDown();
        for ( int i = 0; i < 2; i++ ) {
            Future<Void> completion = completions.poll( 5, SECONDS );
            assertNotNull( "A flush did not finish after the upload completed", completion );
            completion.get();
        }
        assertEquals( 1, uploads.get() );
    }


    @Test
    public void timedCloseCancelsQueuedUploadsAndUnblocksFlush() throws Exception {
        handler.publish( "in flight" );
        assertTrue( uploadStarted.await( 5, SECONDS ) );
        handler.publish( "queued" );

        CountDownLatch flushStarted = new CountDownLatch( 1 );
        Future<?> flush = callers.submit( () -> {
            flushStarted.countDown();
            handler.flush();
            return null;
        } );
        assertTrue( flushStarted.await( 5, SECONDS ) );
        Future<?> close = callers.submit( () -> {
            handler.close( 100, MILLISECONDS );
            return null;
        } );
        close.get( 3, SECONDS );
        flush.get( 3, SECONDS );

        // Keep the HTTP response blocked until both callers have returned.
        allowResponse.countDown();
        handler.publish( "after close" );
        handler.flush();
        assertEquals( 1, uploads.get() );
    }

}

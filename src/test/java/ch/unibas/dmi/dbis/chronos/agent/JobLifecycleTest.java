package ch.unibas.dmi.dbis.chronos.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import kong.unirest.core.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class JobLifecycleTest {

    private HttpServer control;
    private TestAgent agent;
    private volatile String rejectPatch = "";
    private volatile int storedStatus;
    private volatile boolean applyPatchBeforeError;
    private volatile boolean failStatusReadback;
    private volatile boolean omitStatus;
    private final List<String> updates = new CopyOnWriteArrayList<>();
    private volatile boolean rejectZip;


    @Before
    public void startControl() throws IOException {
        InetAddress address = InetAddress.getByName( "127.0.0.1" );
        control = HttpServer.create( new InetSocketAddress( address, 0 ), 0 );
        control.createContext( "/", this::respond );
        control.start();
        agent = new TestAgent( address, control.getAddress().getPort() );
        agent.setSingleJobId( 1 );
    }


    @After
    public void stopControl() {
        control.stop( 0 );
    }


    @Test
    public void rejectedRunningDoesNotStartWork() {
        rejectPatch = "status=2";
        assertJobError();
        assertNull( agent.jobDirectory );
        assertEquals( List.of( "status=2" ), updates );
    }


    @Test
    public void successfulHttpResponseWithoutStatusDoesNotStartWork() {
        omitStatus = true;
        assertJobError();
        assertNull( agent.jobDirectory );
        assertEquals( List.of( "status=2" ), updates );
    }


    @Test
    public void rejectedZipFailsJobBeforePublishingResults() {
        rejectZip = true;
        assertJobError();
        assertTrue( agent.failed );
        assertEquals( List.of( "status=2", "zip", "status=-2" ), updates );
    }


    @Test
    public void rejectedResultMetadataFailsJobAfterZipUpload() {
        rejectPatch = "result";
        assertJobError();
        assertTrue( agent.failed );
        assertEquals( List.of( "status=2", "zip", "result", "status=-2" ), updates );
        assertFalse( agent.jobDirectory.exists() );
    }


    @Test
    public void rejectedFinishedReportsUnconfirmedCompletionAndPreservesStoredStatus() {
        rejectPatch = "status=3";
        assertJobError();
        assertFalse( agent.failed );
        assertEquals( 2, storedStatus );
        assertEquals( List.of( "status=2", "zip", "result", "status=3" ), updates );
    }


    @Test
    public void lostFinishedReplyIsConfirmedByReadingStoredStatus() {
        rejectPatch = "status=3";
        applyPatchBeforeError = true;
        agent.run();
        assertFalse( agent.failed );
        assertEquals( 3, storedStatus );
        assertEquals( List.of( "status=2", "zip", "result", "status=3" ), updates );
        assertFalse( agent.jobDirectory.exists() );
    }


    @Test
    public void failedReadbackLeavesCommittedFinishedStatusIntact() {
        rejectPatch = "status=3";
        applyPatchBeforeError = true;
        failStatusReadback = true;
        assertJobError();
        assertFalse( agent.failed );
        assertEquals( 3, storedStatus );
        assertEquals( List.of( "status=2", "zip", "result", "status=3" ), updates );
        assertFalse( agent.jobDirectory.exists() );
    }


    @Test
    public void confirmedJobFinishesAndCleansUp() {
        agent.run();
        assertEquals( List.of( "status=2", "zip", "result", "status=3" ), updates );
        assertFalse( agent.failed );
        assertFalse( agent.jobDirectory.exists() );
    }


    private void assertJobError() {
        RuntimeException error = assertThrows( RuntimeException.class, agent::run );
        assertTrue( error.toString(), error.getCause() instanceof ChronosException );
    }


    private void respond( HttpExchange exchange ) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            String body = new String( exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8 );
            JSONObject response = new JSONObject().put( "status", new JSONObject().put( "code", 200 ) );
            boolean rejected = false;
            if ( exchange.getRequestMethod().equals( "GET" ) ) {
                rejected = failStatusReadback && updates.contains( "status=3" );
                response.put( "response", new JSONObject()
                        .put( "id", 1 ).put( "user", 1 ).put( "status", storedStatus )
                        .put( "system", "test" ).put( "username", "test" ).put( "name", "job" )
                        .put( "description", "" ).put( "cdl", "" ).put( "created", "" ) );
            } else if ( path.contains( "getUploadTarget" ) ) {
                response.put( "response", new JSONObject().put( "method", "http" )
                        .put( "hostname", "http://127.0.0.1:" + control.getAddress().getPort() ).put( "path", "/uploads" ) );
            } else if ( path.startsWith( "/uploads/" ) ) {
                updates.add( "zip" );
                if ( rejectZip ) {
                    response.getJSONObject( "status" ).put( "code", 600 );
                }
            } else if ( exchange.getRequestMethod().equals( "PATCH" ) ) {
                String update = body.startsWith( "result=" ) ? "result" : body;
                updates.add( update );
                rejected = update.equals( rejectPatch );
                if ( update.startsWith( "status=" ) && (!rejected || applyPatchBeforeError) ) {
                    storedStatus = Integer.parseInt( update.substring( "status=".length() ) );
                }
                if ( omitStatus ) {
                    response = new JSONObject();
                }
            }
            byte[] data = (rejected ? "<html>Bad Gateway</html>" : response.toString()).getBytes( StandardCharsets.UTF_8 );
            exchange.getResponseHeaders().set( "Content-Type", rejected ? "text/html" : "application/json" );
            exchange.sendResponseHeaders( rejected ? 502 : 200, data.length );
            exchange.getResponseBody().write( data );
        } finally {
            exchange.close();
        }
    }


    private static class TestAgent extends AbstractChronosAgent {

        File jobDirectory;
        boolean failed;


        TestAgent( InetAddress address, int port ) {
            super( address, port, false, false );
        }


        @Override
        protected Properties executePhases( ChronosJob job, File input, File output ) {
            jobDirectory = output.getParentFile();
            return new Properties();
        }


        @Override protected String[] getSupportedSystemNames() { return new String[]{ "test" }; }
        @Override protected void failed( ChronosJob job ) { failed = true; }
        @Override protected void aborted( ChronosJob job ) { throw new AssertionError(); }
        @Override protected Object prepare( ChronosJob job, File in, File out, Properties result, Object prior ) { throw new AssertionError(); }
        @Override protected Object warmUp( ChronosJob job, File in, File out, Properties result, Object prior ) { throw new AssertionError(); }
        @Override protected Object execute( ChronosJob job, File in, File out, Properties result, Object prior ) { throw new AssertionError(); }
        @Override protected Object analyze( ChronosJob job, File in, File out, Properties result, Object prior ) { throw new AssertionError(); }
        @Override protected Object clean( ChronosJob job, File in, File out, Properties result, Object prior ) { throw new AssertionError(); }
    }

}

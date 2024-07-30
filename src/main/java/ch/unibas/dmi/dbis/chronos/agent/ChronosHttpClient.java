/*
The MIT License (MIT)

Copyright (c) 2018-2023 The Chronos Project

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

package ch.unibas.dmi.dbis.chronos.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.ContentType;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.UnirestException;
import kong.unirest.json.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.validator.routines.InetAddressValidator;


@Slf4j
public class ChronosHttpClient {

    static {
        Runtime.getRuntime().addShutdownHook(
                new Thread( Unirest::shutDown, ChronosHttpClient.class.getSimpleName() + "-ShutdownHook" )
        );
    }


    private final InetAddress address;
    private final int port;
    private final boolean secure; // true = https | false = http
    private final boolean useHostname;
    private final String environment;

    private final int maxAttempts = 6; // 1 Minute using 10 seconds
    private final long failedAttemptSleepTimeMillis = TimeUnit.SECONDS.toMillis( 10 );


    public ChronosHttpClient( final String ipAddressOrHostName, final int port, final boolean secure ) throws UnknownHostException, IllegalArgumentException {
        this( ipAddressOrHostName, port, secure, null );
    }


    public ChronosHttpClient( final String ipAddressOrHostName, final int port, final boolean secure, final String environment ) throws UnknownHostException, IllegalArgumentException {
        this( ipAddressOrHostName, port, secure, !InetAddressValidator.getInstance().isValid( ipAddressOrHostName ), environment );
    }


    public ChronosHttpClient( final String ipAddressOrHostName, final int port, final boolean secure, final boolean useHostname ) throws UnknownHostException, IllegalArgumentException {
        this( ipAddressOrHostName, port, secure, useHostname, null );
    }


    public ChronosHttpClient( final String ipAddressOrHostName, final int port, final boolean secure, final boolean useHostname, final String environment ) throws UnknownHostException, IllegalArgumentException {
        if ( ipAddressOrHostName == null || ipAddressOrHostName.isEmpty() ) {
            throw new IllegalArgumentException( "ipAddressOrHostName is null or empty." );
        }

        this.address = InetAddress.getByName( ipAddressOrHostName );
        this.port = port;
        this.secure = secure;
        this.useHostname = useHostname;
        this.environment = environment;
    }


    public ChronosHttpClient( final InetAddress address, final int port, final boolean secure, final boolean useHostName ) {
        this( address, port, secure, useHostName, null );
    }


    public ChronosHttpClient( final InetAddress address, final int port, final boolean secure, final boolean useHostName, final String environment ) {
        this.address = address;
        this.port = port;
        this.secure = secure;
        this.useHostname = useHostName;
        this.environment = environment;
    }


    /**
     * @param jobId The ID for the ChronosJob we want to get.
     * @return The ChronosJob corresponding to the given jobId
     * @throws NoSuchElementException If there is no ChronosJob with the given jobId. Chronos Control returned ChronosRestApi.STATUS_CODE__JOB_DOES_NOT_EXIST.
     * @throws ChronosException Other problems regarding Chonos Control. For example, Chronos Control returned ChronosRestApi.STATUS_CODE__ERROR.
     * @throws IOException If no connection to Chronos Control could be established. Check the last Exception and the log for details.
     * @throws InterruptedException If the thread is interrupted while sleeping between the connection attempts.
     */
    public ChronosJob getJob( final int jobId ) throws NoSuchElementException, ChronosException, IOException, InterruptedException {
        int getJobAttempt = 0;
        Exception lastException;

        do {

            try {
                // NOTICE: intentional return!
                return this.doGetJob( jobId );
            } catch ( UnirestException ex ) {
                log.warn( "Attempt #" + getJobAttempt + " failed." + (getJobAttempt < maxAttempts ? " Retrying in " + TimeUnit.MILLISECONDS.toSeconds( failedAttemptSleepTimeMillis ) + " seconds ... " : ""), ex );
                lastException = ex;
            }

            if ( getJobAttempt < maxAttempts ) {
                Thread.sleep( failedAttemptSleepTimeMillis );
            }

        } while ( getJobAttempt++ < maxAttempts );

        throw new IOException( "The maximum number of connection attempts reached. See above WARNINGs for details. The last exception was: ", lastException );
    }


    private ChronosJob doGetJob( final int jobId ) throws NoSuchElementException, UnirestException, ChronosException {
        final Properties query = getQuery( jobId );

        final JSONObject jsonResponse = Unirest.get( getUrl( address, port, ChronosRestApi.JOB, query ) ).asJson().getBody().getObject(); // throws UnirestException

        final JSONObject status = jsonResponse.getJSONObject( ChronosRestApi.STATUS_OBJECT_KEY );

        if ( status.getInt( ChronosRestApi.STATUS_CODE_KEY ) == ChronosRestApi.STATUS_CODE__JOB_DOES_NOT_EXIST ) {
            throw new NoSuchElementException( "Service returned: " + status.getString( ChronosRestApi.STATUS_MESSAGE_KEY ) );
        }

        if ( status.getInt( ChronosRestApi.STATUS_CODE_KEY ) != ChronosRestApi.STATUS_CODE__SUCCESS ) {
            throw new ChronosException( "Service returned: " + status.getString( ChronosRestApi.STATUS_MESSAGE_KEY ) );
        }

        return new ChronosJob( jsonResponse.getJSONObject( ChronosRestApi.RESPONSE_OBJECT_KEY ) );
    }


    /**
     * @param supportedSystemNames List of system names to get only the jobs the system is build for.
     * @throws NoSuchElementException If there is no ChronosJob with the given jobId. Chronos Control returned ChronosRestApi.STATUS_CODE__JOB_DOES_NOT_EXIST.
     * @throws ChronosException Other problems regarding Chonos Control. For example, Chronos Control returned ChronosRestApi.STATUS_CODE__ERROR.
     * @throws IOException If no connection to Chronos Control could be established. Check the last Exception and the log for details.
     * @throws InterruptedException If the thread is interrupted while sleeping between the connection attempts.
     * @see #getNextJob(String[], String, int[])
     */
    public ChronosJob getNextJob( final String[] supportedSystemNames ) throws NoSuchElementException, ChronosException, IOException, InterruptedException {
        return getNextJob( supportedSystemNames, null, new int[0] );
    }


    /**
     * @param supportedSystemNames List of system names to get only the jobs the system is build for.
     * @param environment overloads this.environment if not null or empty; if null or empty, this.environment is used instead.
     * @throws NoSuchElementException If there is no ChronosJob with the given jobId. Chronos Control returned ChronosRestApi.STATUS_CODE__JOB_DOES_NOT_EXIST.
     * @throws ChronosException Other problems regarding Chonos Control. For example, Chronos Control returned ChronosRestApi.STATUS_CODE__ERROR.
     * @throws IOException If no connection to Chronos Control could be established. Check the last Exception and the log for details.
     * @throws InterruptedException If the thread is interrupted while sleeping between the connection attempts.
     * @see #getNextJob(String[], String, int[])
     */
    public ChronosJob getNextJob( final String[] supportedSystemNames, final String environment ) throws NoSuchElementException, ChronosException, IOException, InterruptedException {
        return getNextJob( supportedSystemNames, environment, new int[0] );
    }


    /**
     * @param supportedSystemNames List of system names to get only the jobs the system is build for.
     * @param excludeJobIds List of IDs which are not considered to be the next job for this agent.
     * @throws NoSuchElementException If there is no ChronosJob with the given jobId. Chronos Control returned ChronosRestApi.STATUS_CODE__JOB_DOES_NOT_EXIST.
     * @throws ChronosException Other problems regarding Chonos Control. For example, Chronos Control returned ChronosRestApi.STATUS_CODE__ERROR.
     * @throws IOException If no connection to Chronos Control could be established. Check the last Exception and the log for details.
     * @throws InterruptedException If the thread is interrupted while sleeping between the connection attempts.
     * @see #getNextJob(String[], String, int[])
     */
    public ChronosJob getNextJob( final String[] supportedSystemNames, final int[] excludeJobIds ) throws NoSuchElementException, ChronosException, IOException, InterruptedException {
        return getNextJob( supportedSystemNames, null, excludeJobIds );
    }


    /**
     * @param supportedSystemNames List of system names to get only the jobs the system is build for.
     * @param environment overloads this.environment if not null or empty; if null or empty, this.environment is used instead.
     * @param excludeJobIds List of IDs which are not considered to be the next job for this agent.
     * @throws NoSuchElementException If there is no ChronosJob with the given jobId. Chronos Control returned ChronosRestApi.STATUS_CODE__JOB_DOES_NOT_EXIST.
     * @throws ChronosException Other problems regarding Chonos Control. For example, Chronos Control returned ChronosRestApi.STATUS_CODE__ERROR.
     * @throws IOException If no connection to Chronos Control could be established. Check the last Exception and the log for details.
     * @throws InterruptedException If the thread is interrupted while sleeping between the connection attempts.
     */
    public ChronosJob getNextJob( final String[] supportedSystemNames, String environment, final int[] excludeJobIds ) throws NoSuchElementException, ChronosException, IOException, InterruptedException {
        int getNextJobAttempt = 0;
        Exception lastException;

        do {

            try {
                // NOTICE: intentional return!
                return this.doGetNextJob( supportedSystemNames, environment, excludeJobIds );
            } catch ( UnirestException ex ) {
                log.warn( "Attempt " + getNextJobAttempt + " failed." + (getNextJobAttempt < maxAttempts ? " Retrying in " + TimeUnit.MILLISECONDS.toSeconds( failedAttemptSleepTimeMillis ) + " seconds ... " : ""), ex );
                lastException = ex;
            }

            if ( getNextJobAttempt < maxAttempts ) {
                Thread.sleep( failedAttemptSleepTimeMillis );
            }

        } while ( getNextJobAttempt++ < maxAttempts );

        throw new IOException( "The maximum number of attempts reached. See above WARNINGs for details. The last exception was: ", lastException );
    }


    private ChronosJob doGetNextJob( final String[] supportedSystemNames, String environment, final int[] excludeJobIds ) throws NoSuchElementException, UnirestException, ChronosException {

        if ( supportedSystemNames == null ) {
            throw new NullPointerException( "supportedSystemNames == null" );
        }
        if ( excludeJobIds == null ) {
            throw new NullPointerException( "excludeJobIds == null" );
        }

        if ( environment == null || environment.isEmpty() ) {
            environment = this.environment;
        }

        final Properties query = getQuery();
        query.put( "id", "next" );
        query.put( "supports", String.join( ",", supportedSystemNames ).toLowerCase() );
        query.put( "exclude", String.join( ",", Arrays.stream( excludeJobIds ).sorted().mapToObj( String::valueOf ).toArray( String[]::new ) ) ); // convert the int array to a String array
        if ( environment != null && !environment.isEmpty() ) {
            query.put( "environment", environment );
        }

        final JSONObject jsonResponse = Unirest.get( getUrl( address, port, ChronosRestApi.JOB, query ) ).asJson().getBody().getObject(); // throws UnirestException
        final JSONObject status = jsonResponse.getJSONObject( ChronosRestApi.STATUS_OBJECT_KEY );

        if ( status.getInt( ChronosRestApi.STATUS_CODE_KEY ) == ChronosRestApi.STATUS_CODE__NO_NEXT_JOB ) {
            throw new NoSuchElementException( "Service returned: " + status.getString( ChronosRestApi.STATUS_MESSAGE_KEY ) );
        }

        if ( status.getInt( ChronosRestApi.STATUS_CODE_KEY ) != ChronosRestApi.STATUS_CODE__SUCCESS ) {
            throw new ChronosException( "Service returned: " + status.getString( ChronosRestApi.STATUS_MESSAGE_KEY ) );
        }

        ChronosJob job = new ChronosJob( jsonResponse.getJSONObject( ChronosRestApi.RESPONSE_OBJECT_KEY ) );

        if ( supportedSystemNames.length == 0 ) {
            return job;
        }

        for ( final String supportedSystemName : supportedSystemNames ) {
            if ( job.system.equalsIgnoreCase( supportedSystemName ) ) {
                return job;
            }
        }

        throw new ChronosException( "job.system (\"" + job.system + "\") does not match \"" + Arrays.toString( supportedSystemNames ) + "\"" );
    }


    /**
     * @param job The ChronosJob want to get the status of.
     * @throws NoSuchElementException If the given job does not exist
     * @throws ChronosException Other problems regarding Chonos Control. For example, Chronos Control returned ChronosRestApi.STATUS_CODE__ERROR.
     * @throws IOException If no connection to Chronos Control could be established. Check the last Exception and the log for details.
     * @throws InterruptedException If the thread is interrupted while sleeping between the connection attempts.
     */
    public ChronosHttpClient.JobStatus getStatus( final ChronosJob job ) throws NoSuchElementException, ChronosException, IOException, InterruptedException {
        return this.getJob( job.id ).status;
    }


    /**
     * @param newStatus The new status to set
     * @return true, on successful set, false otherwise
     */
    public boolean setStatus( final ChronosJob job, final JobStatus newStatus ) {
        try {
            final Properties query = getQuery( job );

            final Map<String, Object> parameters = new HashMap<>();
            parameters.put( "status", newStatus.getStatusId() );

            final JSONObject jsonResponse = Unirest.patch( getUrl( address, port, ChronosRestApi.JOB, query ) ).fields( parameters ).asJson().getBody().getObject(); // throws UnirestException
            final JSONObject status = jsonResponse.getJSONObject( ChronosRestApi.STATUS_OBJECT_KEY );

            return status.getInt( ChronosRestApi.STATUS_CODE_KEY ) == ChronosRestApi.STATUS_CODE__SUCCESS;
        } catch ( UnirestException ex ) {
            log.warn( "Unable to send status update to Chronos Control. This attempt will not be repeated by the library." );
            return false;
        }
    }


    /**
     * @param newJobPhase The new job phase to set
     * @return true, on successful set, false otherwise
     */
    public boolean setCurrentJobPhase( final ChronosJob job, final JobPhase newJobPhase ) {
        try {
            final Properties query = getQuery( job );

            final Map<String, Object> parameters = new HashMap<>();
            parameters.put( "currentPhase", newJobPhase.getJobPhaseId() );

            final JSONObject jsonResponse = Unirest.patch( getUrl( address, port, ChronosRestApi.JOB, query ) ).fields( parameters ).asJson().getBody().getObject(); // throws UnirestException
            final JSONObject status = jsonResponse.getJSONObject( ChronosRestApi.STATUS_OBJECT_KEY );

            return status.getInt( ChronosRestApi.STATUS_CODE_KEY ) == ChronosRestApi.STATUS_CODE__SUCCESS;
        } catch ( UnirestException ex ) {
            log.warn( "Unable to report change of job phase to Chronos Control. This attempt will not be repeated by the library." );
            return false;
        }
    }


    /**
     * @param progress Integer [0, 100]. If less than zero it will be set to zero and if greater than 100 it will be set to 100.
     * @return true, on successful set, false otherwise
     */
    public boolean setProgress( final ChronosJob job, byte progress ) {
        return this.setProgress( job.id, progress );
    }


    /**
     * @param progress Integer [0, 100]. If less than zero it will be set to zero and if greater than
     * 100 it will be set to 100.
     * @return true, on successful set, false otherwise
     */
    public boolean setProgress( final int jobId, byte progress ) {
        try {
            final Properties query = getQuery( jobId );

            final Map<String, Object> parameters = new HashMap<>();
            parameters.put( "progress", Math.max( 0, Math.min( progress, 100 ) ) );

            final JSONObject jsonResponse = Unirest.patch( getUrl( address, port, ChronosRestApi.JOB, query ) ).fields( parameters ).asJson().getBody().getObject(); // throws UnirestException
            final JSONObject status = jsonResponse.getJSONObject( ChronosRestApi.STATUS_OBJECT_KEY );

            return status.getInt( ChronosRestApi.STATUS_CODE_KEY ) == ChronosRestApi.STATUS_CODE__SUCCESS;

        } catch ( UnirestException ex ) {
            log.warn( "Unable to send progress update to Chronos Control. This attempt will not be repeated by the library." );
            return false;
        }
    }


    /**
     * @param job The ChronosJob want to get the status of.
     * @param file The file to upload.
     * @param parameters Upload parameters.
     * @throws IllegalArgumentException If job or file are <code>null</code> or if the upload credentials are wrong.
     * @throws NoSuchElementException If the given job does not exist
     * @throws ChronosException Other problems regarding Chonos Control. For example, Chronos Control returned ChronosRestApi.STATUS_CODE__ERROR. Here additionally, if there are issues during the upload to Chronos Control.
     * @throws IOException If no connection to Chronos Control could be established or other I/O related issues. Check the last Exception and the log for details.
     * @throws InterruptedException If the thread is interrupted while sleeping between the connection attempts.
     */
    public void upload( final ChronosJob job, final File file, Properties parameters ) throws IllegalArgumentException, NoSuchElementException, ChronosException, IOException, InterruptedException {
        if ( job == null ) {
            throw new IllegalArgumentException( "job == null" );
        }
        if ( file == null ) {
            throw new IllegalArgumentException( "fileToUpload == null" );
        }
        if ( parameters == null ) {
            parameters = new Properties();
        }

        final Properties uploadConfiguration = getUploadConfiguration( job, file );

        executeUpload( job, file, uploadConfiguration );

        notifyChronos( job, uploadConfiguration, parameters );
    }


    private Properties getUploadConfiguration( final ChronosJob job, final File file ) throws NoSuchElementException, ChronosException, IOException, InterruptedException {
        int getUploadConfigurationAttempt = 0;
        Exception lastException;

        do {

            try {
                // NOTICE: intentional return!
                return this.doGetUploadConfiguration( job, file );
            } catch ( UnirestException ex ) {
                log.warn( "Attempt " + getUploadConfigurationAttempt + " failed." + (getUploadConfigurationAttempt < maxAttempts ? " Retrying in " + TimeUnit.MILLISECONDS.toSeconds( failedAttemptSleepTimeMillis ) + " seconds ... " : ""), ex );
                lastException = ex;
            }

            if ( getUploadConfigurationAttempt < maxAttempts ) {
                Thread.sleep( failedAttemptSleepTimeMillis );
            }

        } while ( getUploadConfigurationAttempt++ < maxAttempts );

        throw new IOException( "The maximum number of attempts reached. See above WARNINGs for details. The last exception was: ", lastException );
    }


    private Properties doGetUploadConfiguration( final ChronosJob job, final File file ) throws NoSuchElementException, ChronosException, UnirestException {
        final Properties query = getQuery( job, "getUploadTarget" );

        final Map<String, Object> parameters = new HashMap<>();
        parameters.put( "filesize", file.length() );

        final JSONObject jsonResponse = Unirest.post( getUrl( address, port, ChronosRestApi.JOB, query ) ).fields( parameters ).asJson().getBody().getObject(); // throws UnirestException
        final JSONObject status = jsonResponse.getJSONObject( ChronosRestApi.STATUS_OBJECT_KEY );

        if ( status.getInt( ChronosRestApi.STATUS_CODE_KEY ) == ChronosRestApi.STATUS_CODE__JOB_DOES_NOT_EXIST ) {
            throw new NoSuchElementException( "Service returned: " + status.getString( ChronosRestApi.STATUS_MESSAGE_KEY ) );
        }

        if ( status.getInt( ChronosRestApi.STATUS_CODE_KEY ) != ChronosRestApi.STATUS_CODE__SUCCESS ) {
            throw new ChronosException( "Service returned: " + status.getString( ChronosRestApi.STATUS_MESSAGE_KEY ) );
        }

        final JSONObject response = jsonResponse.getJSONObject( ChronosRestApi.RESPONSE_OBJECT_KEY );

        final Properties uploadConfiguration = new Properties();
        uploadConfiguration.put( "method", response.getString( "method" ) );
        uploadConfiguration.put( "hostname", response.getString( "hostname" ) );
        uploadConfiguration.put( "path", response.getString( "path" ) );
        if ( response.has( "port" ) ) {
            uploadConfiguration.put( "port", Integer.toString( response.getInt( "port" ) ) );
        }
        if ( response.has( "username" ) ) {
            uploadConfiguration.put( "username", response.getString( "username" ) );
        }
        if ( response.has( "password" ) ) {
            uploadConfiguration.put( "password", response.getString( "password" ) );
        }
        if ( response.has( "filename" ) ) {
            uploadConfiguration.put( "filename", response.getString( "filename" ) );
        }

        return uploadConfiguration;
    }


    private void executeUpload( final ChronosJob job, final File file, final Properties uploadConfiguration ) throws IllegalArgumentException, ChronosException, IOException, InterruptedException {
        switch ( uploadConfiguration.getProperty( "method" ).toLowerCase() ) {
            case "ftp":
                ftpUpload( job, file, uploadConfiguration );
                break;

            case "http":
            case "https":
                httpUpload( job, file, uploadConfiguration );
                break;

            default:
                throw new UnsupportedOperationException( "Upload method \"" + uploadConfiguration.getProperty( "method" ) + " is not supported yet." );

        }
    }


    private void ftpUpload( final ChronosJob job, final File file, final Properties uploadConfiguration ) throws IllegalArgumentException, ChronosException, IOException, InterruptedException {
        log.debug( uploadConfiguration.toString().replaceAll( "password=" + uploadConfiguration.getProperty( "password" ), "password=****" ) );

        final FTPClient client = new FTPClient();

        try ( FileInputStream fis = new FileInputStream( file ) ) {
            int ftpConnectAttempt = 0;
            Exception lastException = null;

            attemptLoop:
            do {

                try {
                    client.connect( uploadConfiguration.getProperty( "hostname" ), Integer.parseInt( uploadConfiguration.getProperty( "port" ) ) );
                    break attemptLoop;
                } catch ( IOException ex ) {
                    log.warn( "Attempt " + ftpConnectAttempt + " failed." + (ftpConnectAttempt < maxAttempts ? " Retrying in " + TimeUnit.MILLISECONDS.toSeconds( failedAttemptSleepTimeMillis ) + " seconds ... " : ""), ex );
                    lastException = ex;
                }

                if ( ftpConnectAttempt < maxAttempts ) {
                    Thread.sleep( failedAttemptSleepTimeMillis );
                }

            } while ( ftpConnectAttempt++ < maxAttempts );

            if ( ftpConnectAttempt >= maxAttempts ) {
                throw new IOException( "The maximum number of attempts reached. See above WARNINGs for details. The last exception was: ", lastException );
            }

            //
            if ( !client.login( uploadConfiguration.getProperty( "username" ), uploadConfiguration.getProperty( "password" ) ) ) {
                throw new IllegalArgumentException( "Login failed. Wrong credentials." );
            }

            client.enterLocalPassiveMode();
            client.setFileType( FTP.BINARY_FILE_TYPE );

            client.changeWorkingDirectory( uploadConfiguration.getProperty( "path" ) );

            log.info( "Storing " + file.getName() + " as " + uploadConfiguration.getProperty( "filename" ) );
            if ( !client.storeFile( uploadConfiguration.getProperty( "filename" ), fis ) ) {
                throw new ChronosException( "Upload of " + file.getName() + " failed: storeFile returned false." );
            }
            client.rename( file.getName(), uploadConfiguration.getProperty( "filename" ) );

            if ( !client.logout() ) {
                log.warn( "FTP logout failed." );
            }
        } finally {
            client.disconnect();
        }
    }


    private void httpUpload( final ChronosJob job, final File file, final Properties uploadConfiguration ) throws IllegalArgumentException, IOException, ChronosException {
        try ( FileInputStream fis = new FileInputStream( file ) ) {
            String url = uploadConfiguration.getProperty( "hostname" ) + uploadConfiguration.getProperty( "path" ) + "/action=upload/id=" + job.id;
            HttpResponse<JsonNode> jsonResponse = Unirest.post( url )
                    .field( "name", "result" )
                    .field( "result", fis, ContentType.APPLICATION_OCTET_STREAM, "results.zip" )
                    .asJson();
            // Get result
            String resultString = jsonResponse.getBody().toString();
            if ( jsonResponse.getStatus() != ChronosRestApi.STATUS_CODE__SUCCESS ) {
                log.warn( resultString );
            }
        } catch ( UnirestException e ) {
            log.warn( "Exception in HTTP upload", e );
            throw new ChronosException( "Exception during HTTP upload", e );
        }
    }


    private boolean notifyChronos( final ChronosJob job, final Properties uploadConfiguration, final Properties parameters ) throws IOException, InterruptedException {
        int norifyChronosAttempt = 0;
        Exception lastException;

        do {
            try {
                // NOTICE: intentional return!
                return this.doNotifyChronos( job, uploadConfiguration, parameters );
            } catch ( UnirestException ex ) {
                log.warn( "Attempt " + norifyChronosAttempt + " failed." + (norifyChronosAttempt < maxAttempts ? " Retrying in " + TimeUnit.MILLISECONDS.toSeconds( failedAttemptSleepTimeMillis ) + " seconds ... " : ""), ex );
                lastException = ex;
            }

            if ( norifyChronosAttempt < maxAttempts ) {
                Thread.sleep( failedAttemptSleepTimeMillis );
            }
        } while ( norifyChronosAttempt++ < maxAttempts );

        throw new IOException( "The maximum number of attempts reached. See above WARNINGs for details. The last exception was: ", lastException );
    }


    private boolean doNotifyChronos( final ChronosJob job, final Properties uploadConfiguration, final Properties parameters ) throws UnirestException {
        final Properties query = getQuery( job ); // query the job

        JSONObject parametersJson = new JSONObject(); // convert parameters to json
        for ( Map.Entry<Object, Object> parameter : parameters.entrySet() ) {
            parametersJson.put( parameter.getKey().toString(), parameter.getValue().toString() );
        }
        final Map<String, Object> queryParameters = new HashMap<>();
        queryParameters.put( "result", parametersJson ); // the PATCH payload

        final JSONObject jsonResponse = Unirest.patch( getUrl( address, port, ChronosRestApi.JOB, query ) ).fields( queryParameters ).asJson().getBody().getObject(); // throws UnirestException
        final JSONObject status = jsonResponse.getJSONObject( ChronosRestApi.STATUS_OBJECT_KEY );

        return status.getInt( ChronosRestApi.STATUS_CODE_KEY ) == ChronosRestApi.STATUS_CODE__SUCCESS;
    }


    private Properties getQuery() {
        return new Properties();
    }


    private Properties getQuery( final ChronosJob job ) {
        return this.getQuery( job.id, null );
    }


    private Properties getQuery( final int jobId ) {
        return this.getQuery( jobId, null );
    }


    private Properties getQuery( final ChronosJob job, final String action ) {
        return this.getQuery( job.id, action );
    }


    private Properties getQuery( final int jobId, final String action ) {
        final Properties query = this.getQuery();
        query.put( "id", jobId );
        if ( action != null && !action.isEmpty() ) {
            query.put( "action", action );
        }
        return query;
    }


    private String getUrl( final InetAddress address, final int port, final ChronosRestApi path, final Properties query ) {
        final StringBuilder url = new StringBuilder();

        // Scheme & Authority
        if ( this.secure ) {
            url.append( "https://" );
        } else {
            //noinspection HttpUrlsUsage
            url.append( "http://" );
        }
        if ( this.useHostname ) {
            url.append( address.getHostName() );
        } else {
            url.append( address.getHostAddress() );
        }
        if ( port != 80 && port != 443 ) {
            url.append( ":" ).append( port );
        }

        // Path
        url.append( path.getPath() );

        // Query
        if ( query != null ) {
            for ( Map.Entry<Object, Object> queryParameter : query.entrySet() ) {
                url.append( "/" ).append( queryParameter.getKey().toString() ).append( "=" ).append( queryParameter.getValue().toString() );
            }
        }

        return url.toString();
    }


    public enum ChronosRestApi {

        JOB( "job" ),
        //
        ;

        public static final String API_PATH_PREFIX = "api";
        public static final String API_VERSION = "1";

        public static final String STATUS_OBJECT_KEY = "status";
        public static final String STATUS_CODE_KEY = "code";
        public static final int STATUS_CODE__SUCCESS = 200;
        public static final int STATUS_CODE__ERROR = 600;
        public static final int STATUS_CODE__NO_NEXT_JOB = 601;
        public static final int STATUS_CODE__JOB_DOES_NOT_EXIST = 602;
        public static final int STATUS_CODE__OUTDATED_VERSION = 631;
        public static final int STATUS_CODE__UNKNOWN_ENVIRONMENT = 632;
        public static final String STATUS_MESSAGE_KEY = "message";

        public static final String RESPONSE_OBJECT_KEY = "response";

        private final String path;


        ChronosRestApi( final String controller ) {
            final StringBuilder pathBuilder = new StringBuilder();

            pathBuilder
                    .append( "/" ).append( API_PATH_PREFIX )
                    .append( "/" ).append( "v" ).append( API_VERSION )
                    .append( "/" ).append( controller );

            this.path = pathBuilder.toString();
        }


        public String getPath() {
            return path;
        }


        @Override
        public String toString() {
            return this.getPath();
        }
    }


    public enum JobPhase {

        UNSET( 0 ),
        PREPARE( 1 ),
        WARM_UP( 2 ),
        EXECUTE( 3 ),
        ANALYZE( 4 ),
        CLEAN( 5 ),
        //
        ;

        private final int phaseId;


        JobPhase( final int statusId ) {
            this.phaseId = statusId;
        }


        /**
         * @return The ChronosHttpClient.JobPhase for the given phaseId
         * @throws NoSuchElementException If there is no ChronosHttpClient.JobPhase for the given phaseId
         */
        public static ChronosHttpClient.JobPhase getJobPhase( final int phaseId ) throws NoSuchElementException {
            for ( ChronosHttpClient.JobPhase s : values() ) {
                if ( s.phaseId == phaseId ) {
                    return s;
                }
            }
            throw new NoSuchElementException();
        }


        public int getJobPhaseId() {
            return phaseId;
        }
    }


    public enum JobStatus {
        SCHEDULED( 0 ),
        SETUP( 1),
        RUNNING( 2 ),
        FINISHED( 3 ),
        ABORTED( -1 ),
        FAILED( -2 ),
        //
        ;

        private final int statusId;


        JobStatus( final int statusId ) {
            this.statusId = statusId;
        }


        /**
         * @return The ChronosHttpClient.JobStatus for the given statusId
         * @throws NoSuchElementException If there is no ChronosHttpClient.JobStatus for the given statusId
         */
        public static ChronosHttpClient.JobStatus getStatus( final int statusId ) throws NoSuchElementException {
            for ( ChronosHttpClient.JobStatus s : values() ) {
                if ( s.statusId == statusId ) {
                    return s;
                }
            }
            throw new NoSuchElementException();
        }


        public int getStatusId() {
            return statusId;
        }
    }


    /**
     *
     */
    public enum JobType {

        DATA( 1 ),
        EVALUATION( 2 ),
        ANALYSIS( 3 ),
        //
        ;

        private final int jobTypeId;


        JobType( final int jobTypeId ) {
            this.jobTypeId = jobTypeId;
        }


        /**
         * @return The ChronosHttpClient.JobType for the given jobTypeId
         * @throws NoSuchElementException If there is no ChronosHttpClient.JobType for the given jobTypeId
         */
        public static ChronosHttpClient.JobType getType( final int jobTypeId ) throws NoSuchElementException {
            for ( ChronosHttpClient.JobType s : values() ) {
                if ( s.jobTypeId == jobTypeId ) {
                    return s;
                }
            }
            throw new NoSuchElementException();
        }


        public int getJobTypeId() {
            return jobTypeId;
        }
    }


    /**
     * Handler which uploads the Java Logging output to Chronos
     */
    public final class ChronosLogHandler {

        private final ExecutorService executor;
        private final Deque<Future<?>> pendingMessages;

        private final Properties query;
        private final Map<String, Object> parameters;

        final AtomicInteger sequenceNumber = new AtomicInteger();

        final static int MAX_RETRIES = 2;


        public ChronosLogHandler( final ChronosJob job ) {
            this.executor = Executors.newSingleThreadExecutor();
            this.pendingMessages = new ConcurrentLinkedDeque<>();

            this.query = getQuery( job, "appendLog" );
            this.parameters = new HashMap<>();
        }


        public void publish( String message ) {
            if ( executor.isShutdown() || executor.isTerminated() ) {
                return;
            }

            pendingMessages.add( executor.submit( () -> {
                int attempt = 0;
                while ( attempt < MAX_RETRIES ) {
                    if ( attempt > 0 ) {
                        try {
                            TimeUnit.MILLISECONDS.sleep( 500 );
                        } catch ( InterruptedException e ) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    try {
                        parameters.clear();
                        parameters.put( "recordSequenceNumber", sequenceNumber.incrementAndGet() );
                        parameters.put( "log", message );

                        JSONObject jsonResponse = Unirest.post( getUrl( address, port, ChronosRestApi.JOB, query ) )
                                .fields( parameters ).asJson().getBody().getObject();
                        JSONObject status = jsonResponse.getJSONObject( ChronosRestApi.STATUS_OBJECT_KEY );

                        if ( status.getInt( ChronosRestApi.STATUS_CODE_KEY ) == ChronosRestApi.STATUS_CODE__SUCCESS ) {
                            return; // Success, exit retry loop
                        } else {
                            log.warn( "Service returned: {}: {}",
                                    status.getInt( ChronosRestApi.STATUS_CODE_KEY ),
                                    status.getString( ChronosRestApi.STATUS_MESSAGE_KEY ) );
                        }
                    } catch ( UnirestException ex ) {
                        log.debug( "Exception while publishing log records. Attempt {}: {}", attempt + 1, MAX_RETRIES + 1, ex );
                    }
                    attempt++;
                }
                log.warn( "Failed to publish log after {} attempts", MAX_RETRIES + 1 );
            } ) );
        }


        public void flush() throws InterruptedException {
            while ( !pendingMessages.isEmpty() ) {
                try {
                    pendingMessages.pollFirst().get();
                } catch ( java.util.concurrent.ExecutionException ex ) {
                    log.warn( "Exception while flushing the log.", ex );
                } catch ( InterruptedException ex ) {
                    log.warn( "Exception while flushing the log.", ex );
                    throw ex;
                }
            }
        }


        public void close() throws SecurityException, InterruptedException {
            executor.shutdown();
            flush();
        }
    }
}

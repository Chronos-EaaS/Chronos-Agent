/*
The MIT License (MIT)

Copyright (c) 2018-2024 The Chronos Project

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


import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient.ChronosLogHandler;
import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient.JobPhase;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.CompressionLevel;
import net.lingala.zip4j.model.enums.CompressionMethod;
import org.apache.commons.io.FileUtils;


/**
 * Abstract Chronos Agent which provides a basic loop containing the fetching, execution and
 * uploading of the results of a job provided by a Chronos HTTP API.
 *
 * If problems with SSL occur: https://confluence.atlassian.com/kb/connecting-to-ssl-services-802171215.html
 * Or use non-secure connections.
 */
@Slf4j
public abstract class AbstractChronosAgent extends Thread {

    private static final long SLEEPING_TIME_VALUE = 10;
    private static final TimeUnit SLEEPING_TIME_UNIT = TimeUnit.SECONDS;

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private final AbortedMonitor abortedMonitor = new AbortedMonitor();
    private final ChronosHttpClient chronos;

    @Getter
    @Setter
    private Integer singleJobId = null;

    private volatile boolean running = true;
    private volatile Thread agent;

    private volatile ChronosHttpClient.ChronosLogHandler chronosLogHandler;


    protected AbstractChronosAgent( final InetAddress address, final int port, final boolean secure, final boolean useHostname ) {
        this.chronos = new ChronosHttpClient( address, port, secure, useHostname );
    }


    protected AbstractChronosAgent( final InetAddress address, final int port, final boolean secure, final boolean useHostname, final String environment ) {
        this.chronos = new ChronosHttpClient( address, port, secure, useHostname, environment );
    }


    protected AbstractChronosAgent( final String ipAddressOrHostname, final int port, final boolean secure ) throws UnknownHostException {
        this.chronos = new ChronosHttpClient( ipAddressOrHostname, port, secure );
    }


    protected AbstractChronosAgent( final String ipAddressOrHostname, final int port, final boolean secure, final String environment ) throws UnknownHostException {
        this.chronos = new ChronosHttpClient( ipAddressOrHostname, port, secure, environment );
    }


    protected AbstractChronosAgent( final String address, final int port, final boolean secure, final boolean useHostname ) throws UnknownHostException {
        this.chronos = new ChronosHttpClient( address, port, secure, useHostname );
    }


    protected AbstractChronosAgent( final String address, final int port, final boolean secure, final boolean useHostname, final String environment ) throws UnknownHostException {
        this.chronos = new ChronosHttpClient( address, port, secure, useHostname, environment );
    }


    /**
     * The agent's execution loop:
     * (1) Requesting new job
     * (2) Set the job's status to RUNNING
     * (3) --
     * (4) Create environment (e.g. in/output folders)
     * (4.9) Add chronos-push-logger
     * (5) Execute the job
     * (5.1) Register the job at the observer
     * (5.2) Actual execution by calling execute
     * (5.3) --
     * (5.4) Save the Properties as json
     * (5.5) Save the cdl (archiving)
     * (5.6) Build package
     * (5.7) Zipping
     * (5.8) Upload
     * (5.9) Job is done
     * (5.e) Set the execution status to FAILED in case of Exceptions
     * (5.10) De-register the job at the observer
     * (5.11) Remove the logger
     * (6) --
     * (7) Delete environment
     */
    @Override
    public void run() {
        this.agent = Thread.currentThread();

        try {
            boolean alreadyPrintedWaitingForJob = false;
            // TODO: this method needs refactoring!
            mainLoop:
            while ( running ) {
                if ( Thread.currentThread().isInterrupted() ) {
                    log.debug( "Ending mainLoop. Reason: Interrupt flag is set." );
                    this.running = false;
                    break mainLoop;
                }

                // (1) Requesting new job
                final ChronosJob job;
                if ( singleJobId == null ) {
                    try {
                        if ( !alreadyPrintedWaitingForJob ) {
                            log.info( "Requesting new job." );
                        }
                        job = this.chronos.getNextJob( getSupportedSystemNames(), getEnvironment() ); // throws NoSuchElementException, ChronosException, IOException, InterruptedException
                    } catch ( NoSuchElementException ex ) {
                        if ( !alreadyPrintedWaitingForJob ) {
                            log.debug( "No job scheduled.", ex );
                            System.out.print( "Waiting for job" );
                            alreadyPrintedWaitingForJob = true;
                        } else {
                            System.out.print( "." );
                        }

                        try {
                            SLEEPING_TIME_UNIT.sleep( SLEEPING_TIME_VALUE );
                        } catch ( InterruptedException ignored2 ) {
                            // Ignore. Maybe this agent is to be shutdown.
                        }

                        continue mainLoop; // !! Important !! -- Reloop

                    } catch ( Exception ex ) {
                        log.error( "IOException for chronos.getNextJob(" + Arrays.toString( getSupportedSystemNames() ) + "," + getEnvironment() + ")", ex );

                        try {
                            SLEEPING_TIME_UNIT.sleep( SLEEPING_TIME_VALUE );
                        } catch ( InterruptedException ignored ) {
                            // Ignore. Maybe this agent is to be shutdown.
                        }

                        alreadyPrintedWaitingForJob = false;
                        continue mainLoop; // !! Important !! -- Reloop

                    }
                } else {
                    job = this.chronos.getJob( singleJobId );
                    this.running = false; // only execute loop once
                }
                alreadyPrintedWaitingForJob = false;

                assert job != null;

                // (2) Set the job's status to RUNNING
                if ( !this.chronos.setStatus( job, ChronosHttpClient.JobStatus.RUNNING ) ) {
                    log.warn( "Cannot set JobStatus to RUNNING. ChronosHttpClient.setStatus returned false." );
                    // TODO: Throw some exception instead?
                }

                // (3) --

                // (4) Create environment
                final File tempDirectory = Files.createTempDirectory( "chronos" ).toFile();
                tempDirectory.deleteOnExit();

                final File inputDirectory = new File( tempDirectory, "input" );
                final File outputDirectory = new File( tempDirectory, "output" );
                final File outputZipFile = new File( tempDirectory, outputDirectory.getName() + ".zip" );
                if ( !inputDirectory.mkdirs() ) {
                    throw new IllegalStateException( "Creation of \"" + inputDirectory.getAbsolutePath() + "\" failed." );
                }
                if ( !outputDirectory.mkdirs() ) {
                    throw new IllegalStateException( "Creation of \"" + outputDirectory.getAbsolutePath() + "\" failed." );
                }
                inputDirectory.deleteOnExit();
                outputDirectory.deleteOnExit();
                outputZipFile.deleteOnExit();

                // (4.9) Add logger
                final ChronosHttpClient.ChronosLogHandler chronosLogHandler = this.chronos.new ChronosLogHandler( job );
                addChronosLogHandler( chronosLogHandler );

                // (5) Execute the job
                try {
                    log.info( job.toString() + " has now the state RUNNING." );
                    final Properties executionResults;

                    // (5.1) Register the job at the observer
                    this.abortedMonitor.observe( job );

                    // (5.2) Actual execution
                    // EXECUTE THE PHASES
                    executionResults = this.executePhases( job, inputDirectory, outputDirectory );

                    // (5.3) --

                    // (5.4) Save the Properties as json
                    this.saveResults( executionResults, outputDirectory );

                    // (5.5) Save the cdl (archiving)
                    this.saveCdl( job, outputDirectory );

                    // (5.6) Build package
                    try {
                        this.copyResults( job, outputDirectory );
                    } catch ( IOException ex ) {
                        log.warn( "Exception storing the results locally.", ex );
                    }

                    // (5.7) Zipping
                    final Properties zipResults = new Properties();
                    zipResults.putAll( executionResults );
                    zipResults.putAll( this.zip( job, outputDirectory, outputZipFile ) );

                    // (5.8) Upload
                    log.info( "Uploading results for " + job.toString() );
                    this.chronos.upload( job, outputZipFile, zipResults ); // throws IllegalArgumentException, NoSuchElementException, ChronosException, IOException, InterruptedException

                    // (5.9) Job is done
                    this.chronos.setStatus( job, ChronosHttpClient.JobStatus.FINISHED );
                    log.info( job.toString() + " has now the state FINISHED." );

                } catch ( InterruptedException ex ) {
                    // (5.e) Reset execution status to ABORTED since we have been interrupted
                    log.warn( "Job " + job.toString() + " FAILED. Reason is:", ex );

                    if ( !this.chronos.setStatus( job, ChronosHttpClient.JobStatus.ABORTED ) ) {
                        log.error( "Cannot reset job " + job.toString() + " to status ABORTED." );
                    }
                    this.aborted( job );

                    throw ex; // "Notify" higher levels

                } catch ( Exception ex ) {
                    // (5.e) Reset execution status to FAILED for various reasons: ExecutionException, job is not accepted, etc.
                    log.warn( "Job " + job.toString() + " FAILED. Reason is:", ex );

                    if ( !this.chronos.setStatus( job, ChronosHttpClient.JobStatus.FAILED ) ) {
                        log.error( "Cannot reset job " + job.toString() + " to status FAILED." );
                    }
                    this.failed( job );

                    if ( singleJobId != null ) {
                        throw ex;
                    }
                } finally {
                    // (5.10) De-register the job at the observer
                    try {
                        this.abortedMonitor.cancelObservation( job );
                    } catch ( NoSuchElementException ex ) {
                        // There the job was not in the tasks list
                        log.debug( "This job was not observed.", ex );
                    }

                    // (5.11) Remove the logger
                    removeChronosLogHandler( chronosLogHandler );
                }

                // (6) --

                // (7) Delete environment
                FileUtils.deleteQuietly( outputZipFile );
                FileUtils.deleteQuietly( outputDirectory );
                FileUtils.deleteQuietly( inputDirectory );
                FileUtils.deleteQuietly( tempDirectory );
            } // mainLoop

        } catch ( InterruptedException ex ) {
            log.warn( "The chronos agent has been interrupted!", ex );
            Thread.currentThread().interrupt();
            if ( singleJobId != null ) {
                throw new RuntimeException( ex );
            }
        } catch ( RuntimeException ex ) {
            log.error( "Unhandled RuntimeException! Will be re-thrown!", ex );
            throw ex;
        } catch ( Exception ex ) {
            log.error( "Unhandled Exception!", ex );
            if ( singleJobId != null ) {
                throw new RuntimeException( ex );
            }
        }

        this.agent = null;
    }


    /**
     * Shuts this agent down; interrupts if necessary.
     */
    public final void shutdown() {
        this.running = false;

        if ( this.agent != null ) {
            this.agent.interrupt();
        }
    }


    /**
     * @return The supported system name which is used for getNextJob (supportedSystemName)
     */
    protected abstract String[] getSupportedSystemNames();


    /**
     * @return The environment which is used for getNextJob (environment filter)
     */
    protected String getEnvironment() {
        return null;
    }


    /**
     * Executes the phases
     * (1) PREPARE
     * (2) WARM_UP
     * (3) EXECUTE
     * (4) ANALYZE
     * (5) CLEAN
     * in a chain, i.e., passes the returned data object of a phase over as an input parameter of the
     * successor phase (usually the direct successor, however, phases can be omitted)
     */
    protected Properties executePhases( final ChronosJob job, final File inputDirectory, final File outputDirectory ) throws ExecutionException {

        long startTime;
        final Properties results = new Properties();

        final Object preparePhaseData;
        if ( (job.phases & ChronosJob.EXCLUDE_PREPARE_PHASE) == ChronosJob.EXCLUDE_PREPARE_PHASE ) {
            log.info( "Skipping PREPARE phase." );
            preparePhaseData = null;
        } else {
            if ( chronos.setCurrentJobPhase( job, JobPhase.PREPARE ) == false ) {
                log.warn( "Could not set job phase." );
            }
            log.info( "Executing PREPARE phase." );
            // START TIME MEASUREMENT
            startTime = System.currentTimeMillis();
            preparePhaseData = prepare( job, inputDirectory, outputDirectory, results, null );
            results.setProperty( "internal.durations.prepare", Long.toString( System.currentTimeMillis() - startTime ) );
            // END TIME MEASUREMENT
        }

        final Object warmUpPhaseData;
        if ( (job.phases & ChronosJob.EXCLUDE_WARM_UP_PHASE) == ChronosJob.EXCLUDE_WARM_UP_PHASE ) {
            log.info( "Skipping WARM_UP phase." );
            warmUpPhaseData = preparePhaseData;
        } else {
            if ( chronos.setCurrentJobPhase( job, JobPhase.WARM_UP ) == false ) {
                log.warn( "Could not set job phase." );
            }
            log.info( "Executing WARM_UP phase." );
            // START TIME MEASUREMENT
            startTime = System.currentTimeMillis();
            warmUpPhaseData = warmUp( job, inputDirectory, outputDirectory, results, preparePhaseData );
            results.setProperty( "internal.durations.warmUp", Long.toString( System.currentTimeMillis() - startTime ) );
            // END TIME MEASUREMENT
        }

        final Object executePhaseData;
        if ( (job.phases & ChronosJob.EXCLUDE_EXECUTE_PHASE) == ChronosJob.EXCLUDE_EXECUTE_PHASE ) {
            log.info( "Skipping EXECUTE phase." );
            executePhaseData = warmUpPhaseData;
        } else {
            if ( chronos.setCurrentJobPhase( job, JobPhase.EXECUTE ) == false ) {
                log.warn( "Could not set job phase." );
            }
            log.info( "Executing EXECUTE phase." );
            // START TIME MEASUREMENT
            startTime = System.currentTimeMillis();
            executePhaseData = execute( job, inputDirectory, outputDirectory, results, warmUpPhaseData );
            results.setProperty( "internal.durations.execute", Long.toString( System.currentTimeMillis() - startTime ) );
            // END TIME MEASUREMENT
        }

        final Object analyzePhaseData;
        if ( (job.phases & ChronosJob.EXCLUDE_ANALYZE_PHASE) == ChronosJob.EXCLUDE_ANALYZE_PHASE ) {
            log.info( "Skipping ANALYZE phase." );
            analyzePhaseData = executePhaseData;
        } else {
            if ( chronos.setCurrentJobPhase( job, JobPhase.ANALYZE ) == false ) {
                log.warn( "Could not set job phase." );
            }
            log.info( "Executing ANALYZE phase." );
            // START TIME MEASUREMENT
            startTime = System.currentTimeMillis();
            analyzePhaseData = analyze( job, inputDirectory, outputDirectory, results, executePhaseData );
            results.setProperty( "internal.durations.analyze", Long.toString( System.currentTimeMillis() - startTime ) );
            // END TIME MEASUREMENT
        }

        if ( (job.phases & ChronosJob.EXCLUDE_CLEAN_PHASE) == ChronosJob.EXCLUDE_CLEAN_PHASE ) {
            log.info( "Skipping CLEAN phase." );
        } else {
            if ( chronos.setCurrentJobPhase( job, JobPhase.CLEAN ) == false ) {
                log.warn( "Could not set job phase." );
            }
            log.info( "Executing CLEAN phase." );
            // START TIME MEASUREMENT
            startTime = System.currentTimeMillis();
            clean( job, inputDirectory, outputDirectory, results, analyzePhaseData );
            results.setProperty( "internal.durations.clean", Long.toString( System.currentTimeMillis() - startTime ) );
            // END TIME MEASUREMENT
        }

        return results;
    }


    /**
     * @param inputDirectory Temporary input directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param outputDirectory Temporary output directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param results Key-Value results which are uploaded as json-file
     * @param prePhaseData Implementation specific data exchange object from the previous ?yet unknown? phase -- typically null
     * @return Implementation specific data exchange object which is passed to the next (typically warmUp) phase parameter "prePhaseData"
     * @throws ExecutionException Can be thrown by the implementation; leads to the job state FAILED
     */
    protected abstract Object prepare(
            final ChronosJob job,
            final File inputDirectory,
            final File outputDirectory,
            final Properties results,
            final Object prePhaseData ) throws ExecutionException;

    /**
     * @param inputDirectory Temporary input directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param outputDirectory Temporary output directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param results Key-Value results which are uploaded as json-file
     * @param prePhaseData Implementation specific data exchange object from the previous (typically prepare) phase
     * @return Implementation specific data exchange object which is passed to the next (typically execute) phase parameter "prePhaseData"
     * @throws ExecutionException Can be thrown by the implementation; leads to the job state FAILED
     */
    protected abstract Object warmUp(
            final ChronosJob job,
            final File inputDirectory,
            final File outputDirectory,
            final Properties results,
            final Object prePhaseData ) throws ExecutionException;

    /**
     * The core method which is needed to be implemented by subclasses.
     * Here, the job will be executed (according) to the job.cdl content.
     *
     * @param inputDirectory Temporary input directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param outputDirectory Temporary output directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param results Key-Value results which are uploaded as json-file
     * @param prePhaseData Implementation specific data exchange object from the previous (typically warmUp) phase
     * @return Implementation specific data exchange object which is passed to the next (typically analyze) phase parameter "prePhaseData"
     * @throws ExecutionException Can be thrown by the implementation; leads to the job state FAILED
     */
    protected abstract Object execute(
            final ChronosJob job,
            final File inputDirectory,
            final File outputDirectory,
            final Properties results,
            final Object prePhaseData ) throws ExecutionException;

    /**
     * @param inputDirectory Temporary input directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param outputDirectory Temporary output directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param results Key-Value results which are uploaded as json-file
     * @param prePhaseData Implementation specific data exchange object from the previous (typically execute) phase
     * @return Implementation specific data exchange object which is passed to the next (typically clean) phase parameter "prePhaseData"
     * @throws ExecutionException Can be thrown by the implementation; leads to the job state FAILED
     */
    protected abstract Object analyze(
            final ChronosJob job,
            final File inputDirectory,
            final File outputDirectory,
            final Properties results,
            final Object prePhaseData ) throws ExecutionException;

    /**
     * @param inputDirectory Temporary input directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param outputDirectory Temporary output directory. Will be deleted by the environment after the (successful|failed|aborted) execution.
     * @param results Key-Value results which are uploaded as json-file
     * @param prePhaseData Implementation specific data exchange object from the previous (typically analyze) phase
     * @return Implementation specific data exchange object which is passed to the ?yet unknown - currently ignored? phase parameter "prePhaseData"
     * @throws ExecutionException Can be thrown by the implementation; leads to the job state FAILED
     */
    protected abstract Object clean(
            final ChronosJob job,
            final File inputDirectory,
            final File outputDirectory,
            final Properties results,
            final Object prePhaseData ) throws ExecutionException;


    /**
     * Saves the Key-Value results in results.json [json-encoded]
     *
     * @throws IllegalStateException in case of a FileNotFoundException
     * @throws RuntimeException in case of an UnsupportedEncodingException which is thrown if UTF-8 is not supported.
     */
    protected void saveResults( final Properties executionResults, final File outputDirectory ) throws IllegalStateException {
        final File resultsJsonFile = new File( outputDirectory, "results.json" );
        Utils.saveResults( executionResults, resultsJsonFile );
    }


    /**
     * Stores the job's cdl in the {outputDirectory}/job.cdl file.
     *
     * @throws IllegalStateException in case of a FileNotFoundException
     * @throws RuntimeException in case of an UnsupportedEncodingException which is thrown if UTF-8 is not supported.
     */
    protected void saveCdl( final ChronosJob job, final File outputDirectory ) throws IllegalStateException {
        final File cdlFile = new File( outputDirectory, "job.cdl" );

        try ( PrintWriter out = new PrintWriter( cdlFile, UTF_8.name() ) ) {
            out.println( job.cdl );
            out.flush();
        } catch ( FileNotFoundException ex ) {
            throw new IllegalStateException( ex );
        } catch ( UnsupportedEncodingException ex ) {
            throw new RuntimeException( ex );
        }
    }


    /**
     * Overwrite this method to copy the results in outputDirectory to a custom location.
     * Notice: the outputDirectory is anyway zipped and uploaded!
     *
     * @throws IOException Can be thrown in case of exceptions during the copy process.
     */
    protected void copyResults( final ChronosJob job, final File outputDirectory ) throws IOException {
    }


    private Properties zip( final ChronosJob job, final File outputDirectory, final File outputZipFile ) throws ExecutionException {
        final Properties results = new Properties();

        ZipParameters zipParams = new ZipParameters();
        zipParams.setCompressionMethod( CompressionMethod.DEFLATE );
        zipParams.setCompressionLevel( CompressionLevel.NORMAL );

        log.info( "Zipping results." );
        try ( ZipFile outputZip = new ZipFile( outputZipFile ) ) {
            outputZip.addFolder( outputDirectory, zipParams );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        return results;
    }


    /**
     * @param progress Integer [0, 100]. If less than zero it will be set to zero and if greater than 100 it will be set to 100.
     * @return true, on successful set
     * @throws IllegalArgumentException If job == null
     */
    protected boolean setProgress( final ChronosJob job, byte progress ) throws IllegalArgumentException {
        if ( job == null ) {
            throw new IllegalArgumentException( "ChronosJob job == null" );
        }

        return this.setProgress( job.id, progress );
    }


    /**
     * @param progress Integer [0, 100]. If less than zero it will be set to zero and if greater than 100 it will be set to 100.
     * @return true, on successful set
     */
    protected boolean setProgress( final int jobId, byte progress ) {
        return this.chronos.setProgress( jobId, (byte) Math.max( 0, Math.min( progress, 100 ) ) );
    }


    /**
     * Method which is called by the monitor if the given job was aborted.
     */
    protected abstract void aborted( final ChronosJob abortedJob );


    /**
     * Method which is called if the job failed.
     */
    protected abstract void failed( final ChronosJob failedJob );


    /**
     * Usually, the subclasses and other classes do not need to call this method. It is only required
     * if the run()-method is overwritten (with no super.run() call in it) and one does want to have
     * the fancy log-push feature.
     */
    protected void addChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        // Do nothing
    }


    /**
     * Usually, the subclasses and other classes do not need to call this method. It is only required
     * if the run()-method is overwritten (with no super.run() call in it) and one does want to have
     * the fancy log-push feature.
     */
    protected void removeChronosLogHandler( final ChronosHttpClient.ChronosLogHandler chronosLogHandler ) {
        // Do nothing
    }


    /**
     * Watches if the job to observe is aborted/canceled at the Chronos website.
     * The job's state is fetched every 10 seconds (default) and compared against ABORTED
     */
    private class AbortedMonitor {

        private final Timer timer = new Timer( AbortedMonitor.class.getSimpleName(), true );
        private final Map<ChronosJob, AbortedMonitorTask> tasks = new ConcurrentHashMap<>();


        /**
         * Calls the <code>observe(ChronosJob, long)</code> method with the default sleeping time.
         *
         * @see #observe(ChronosJob, long)
         * @see AbstractChronosAgent#SLEEPING_TIME_VALUE
         * @see AbstractChronosAgent#SLEEPING_TIME_UNIT
         */
        public void observe( final ChronosJob observable ) {
            observe( observable, SLEEPING_TIME_UNIT.toMillis( SLEEPING_TIME_VALUE ) );
        }


        /**
         * @param periodMillis Time in millis between two JobStatus checks
         */
        public void observe( final ChronosJob observable, final long periodMillis ) {
            final AbortedMonitorTask task = new AbortedMonitorTask( observable );
            this.tasks.put( observable, task );
            this.timer.schedule( task, TimeUnit.SECONDS.toMillis( 0 ), periodMillis );
        }


        /**
         * @return successful cancellation
         * @see TimerTask#cancel()
         */
        public boolean cancelObservation( final ChronosJob observable ) throws NoSuchElementException {
            final AbortedMonitorTask task = this.tasks.remove( observable );
            if ( task == null ) {
                throw new NoSuchElementException( "this.tasks.remove(observable) returned null" );
            }
            return task.cancel();
        }


        /**
         * The actual monitoring task
         */
        private class AbortedMonitorTask extends TimerTask {

            private final ChronosJob observable;

            private final AtomicInteger getStatusErrorCounter = new AtomicInteger( 0 );


            /**
             *
             */
            public AbortedMonitorTask( final ChronosJob observable ) {
                this.observable = observable;
            }


            /**
             * (1) Checks if the JobStatus is ABORTED.
             *
             * (2a) It is: Calls the AbstractChronosAgent.aborted(ChronosJob) method.
             *
             * (3) De-registers and cancels this task [also, if the JobStatus is FINISHED or in case of exceptions]
             *
             * @see AbstractChronosAgent#aborted(ChronosJob)
             */
            @Override
            public void run() {
                try {
                    // Fetch status and compare if ABORTED
                    final ChronosHttpClient.JobStatus jobStatus = AbstractChronosAgent.this.chronos.getStatus( this.observable );
                    if ( jobStatus == ChronosHttpClient.JobStatus.ABORTED ) {
                        log.warn( "Aborting job {}", this.observable.id );
                        AbstractChronosAgent.this.aborted( this.observable );
                        cancelAndRemoveObservable();
                    }
                    if ( jobStatus == ChronosHttpClient.JobStatus.FINISHED ) {
                        // Quietly cancelObservation and de-register this observable since it is already finished
                        cancelAndRemoveObservable();
                    }
                    getStatusErrorCounter.set( 0 );
                } catch ( NoSuchElementException | ChronosException | IOException ex ) {
                    getStatusErrorCounter.incrementAndGet();
                    if ( getStatusErrorCounter.get() % 10 == 0 ) {
                        log.warn( "Unable to get status of \"" + this.observable + "\" since quite a while.", ex );
                    }
                    //cancelAndRemoveObservable();
                } catch ( InterruptedException ex ) {
                    log.warn( "We have been interrupted!", ex );
                    cancelAndRemoveObservable();
                    Thread.currentThread().interrupt();
                }
            }


            private void cancelAndRemoveObservable() {
                AbortedMonitor.this.tasks.remove( this.observable );
                this.cancel();
            }

        }

    }

}

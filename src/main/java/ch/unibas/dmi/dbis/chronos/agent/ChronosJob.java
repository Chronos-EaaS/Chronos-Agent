/*
The MIT License (MIT)

Copyright (c) 2018 Databases and Information Systems Research Group, University of Basel, Switzerland

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


import java.io.Serializable;
import java.util.Objects;
import org.json.JSONObject;


/**
 * Representation of a Job of the Chronos System.
 *
 * @author Alexander Stiemer (alexander.stiemer@unibas.ch)
 * @author Marco Vogt (marco.vogt@unibas.ch)
 */
public class ChronosJob implements Serializable {


    public static final int EXCLUDE_PREPARE_PHASE = 0b00001; // 1
    public static final int EXCLUDE_WARM_UP_PHASE = 0b00010; // 2
    public static final int EXCLUDE_EXECUTE_PHASE = 0b00100; // 4
    public static final int EXCLUDE_ANALYZE_PHASE = 0b01000; // 8
    public static final int EXCLUDE_CLEAN_PHASE = 0b10000; // 16
    private static final long serialVersionUID = -7334163605754493258L;
    // Job id
    public final int id;
    // Job name
    public final String name;
    // Job description
    public final String description;
    // Name of the Chronos subsystem
    public final String system;
    // Environment
    public final String environment;
    // Username which created the job
    public final String username;
    // User Id
    public final int userId;
    // Job type
    public final ChronosHttpClient.JobType jobType;
    // Chronos Description Language (the actual job)
    public final String cdl;
    //
    public final ChronosHttpClient.JobStatus status;
    //
    public final String created;
    //
    public final String started;
    //
    public final String finished;
    //
    public final int phases;


    protected ChronosJob( final JSONObject job ) {
        this.id = job.getInt( "id" );

        this.username = job.getString( "username" );
        this.userId = job.getInt( "user" );

        this.name = job.getString( "name" );
        this.description = job.getString( "description" );

        this.jobType = ChronosHttpClient.JobType.getType( job.getInt( "type" ) );
        this.system = job.getString( "system" ).toLowerCase(); // toLowerCase - since systems ('uniquename') is always lowercase
        this.environment = job.isNull( "environment" ) ? null : job.getString( "environment" );

        this.cdl = job.getString( "cdl" );

        this.status = ChronosHttpClient.JobStatus.getStatus( job.getInt( "status" ) );

        this.created = job.getString( "created" );
        this.started = job.isNull( "started" ) ? "" : job.getString( "started" );
        this.finished = job.isNull( "finished" ) ? "" : job.getString( "finished" );

        this.phases = job.isNull( "phases" ) ? 0 : job.getInt( "phases" ); // Default: execute all phases
    }


    /**
     * Copy-Constructor
     */
    protected ChronosJob( final ChronosJob job ) {
        this.id = job.id;

        this.username = job.username;
        this.userId = job.userId;

        this.name = job.name;
        this.description = job.description;

        this.jobType = job.jobType;
        this.system = job.system;
        this.environment = job.environment;

        this.cdl = job.cdl;

        this.status = job.status;

        this.created = job.created;
        this.started = job.started;
        this.finished = job.finished;

        this.phases = job.phases;
    }


    @Override
    public boolean equals( Object obj ) {
        if ( obj == null ) {
            return false;
        }
        if ( getClass() != obj.getClass() ) {
            return false;
        }
        final ChronosJob other = (ChronosJob) obj;
        return this.id == other.id;
    }


    @Override
    public int hashCode() {
        int hash = 5;
        hash = 79 * hash + this.id;
        hash = 79 * hash + Objects.hashCode( this.name );
        hash = 79 * hash + Objects.hashCode( this.description );
        hash = 79 * hash + Objects.hashCode( this.system );
        hash = 79 * hash + Objects.hashCode( this.username );
        hash = 79 * hash + Objects.hashCode( this.cdl );
        return hash;
    }


    /**
     * Uses id, name, system and username for textual representation. Thus, it omits description and cdl.
     */
    @Override
    public String toString() {
        return "ChronosJob{" + "id=" + id + ", name=" + name + ", system=" + system + ", username=" + username + '}';
    }
}

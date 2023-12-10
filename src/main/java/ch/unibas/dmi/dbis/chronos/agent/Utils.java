/*
The MIT License (MIT)

Copyright (c) 2018-2022 The Chronos Project

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
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kong.unirest.json.JSONArray;
import kong.unirest.json.JSONObject;

class Utils {

    static void saveResults( final Properties executionResults, final File resultsJsonFile ) throws IllegalStateException {
        JSONObject resultsJsonObject = new JSONObject();
        for ( Map.Entry<Object, Object> result : executionResults.entrySet() ) {
            Object obj = result.getValue();
            if ( obj instanceof List<?> ) {
                resultsJsonObject.put( result.getKey().toString(), new JSONArray( (List<?>) obj ) );
            } else {
                resultsJsonObject.put( result.getKey().toString(), obj.toString() );
            }
        }

        try ( PrintWriter out = new PrintWriter( resultsJsonFile, StandardCharsets.UTF_8.name() ) ) {
            out.println( resultsJsonObject );
            out.flush();
        } catch ( FileNotFoundException ex ) {
            throw new IllegalStateException( ex );
        } catch ( UnsupportedEncodingException ex ) {
            throw new RuntimeException( ex );
        }
    }

}

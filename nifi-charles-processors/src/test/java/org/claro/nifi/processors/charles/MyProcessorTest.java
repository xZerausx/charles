/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.claro.nifi.processors.charles;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


public class MyProcessorTest {

    private TestRunner runner;

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(MyProcessor.class);
    }


    @Test
    public void TestCSV() throws InitializationException, IOException {
        Servicios.CSVReader(runner);
        Servicios.CSVWriter( runner );
        setProperty();
        runner.clearTransferState();

        runner.enqueue( Paths.get( "src/test/inFlowFile"));
        runner.run();

        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/expectedOutput")));
        runner.getFlowFilesForRelationship(MyProcessor.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    private void setProperty (){
        runner.setProperty( MyProcessor.RECORD_READER, "reader" );
        runner.setProperty( MyProcessor.RECORD_WRITER, "writer" );
    }

}

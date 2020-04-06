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

import jdk.nashorn.internal.lookup.MethodHandleFactory;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;

@Tags({"custom","prueba"})
@CapabilityDescription("Provide a description")
//@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
        @WritesAttribute(attribute="record.count", description="The number of records in an outgoing FlowFile"),
        @WritesAttribute(attribute="mime.type", description="The MIME Type that the configured Record Writer indicates is appropriate")
})
public class MyProcessor extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name( "record-reader" )
            .displayName( "Record Reader" )
            .description( "Specifies the Controller Service to use for reading incoming data" )
            .identifiesControllerService( RecordReaderFactory.class )
            .required( true )
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name( "record-writer" )
            .displayName( "Record Writer" )
            .description( "Specifies the Controller Service to use for writing out the records" )
            .identifiesControllerService( RecordSetWriterFactory.class )
            .required( true )
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name( "success" )
            .description( "FlowFiles that are successfully partitioned will be routed to this relationship" )
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name( "failure" )
            .description( "If a FlowFile cannot be partitioned from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship" )
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add( RECORD_READER );
        descriptors.add( RECORD_WRITER );
        this.descriptors = Collections.unmodifiableList( descriptors );

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add( REL_SUCCESS );
        relationships.add( REL_FAILURE );
        this.relationships = Collections.unmodifiableSet( relationships );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final ComponentLog logger = getLogger();
        final Map<String, String> originalAttributes = flowFile.getAttributes();


        try (final InputStream inStream = session.read(flowFile);
             final RecordReader reader = readerFactory.createRecordReader(flowFile, inStream, getLogger())) {
            try {
                final FlowFile outFlowFile = session.create(flowFile);
                final OutputStream outStream = session.write(outFlowFile);
                final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, reader.getSchema());
                final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, outStream,Collections.emptyMap());

                try {
                    writer.beginRecordSet();
                    Record record;
                    while ((record = reader.nextRecord()) != null) {

                        if (record.getValue("chargingCharacteristics").equals("400") &&
                                record.getValue("billing").equals("Y") ){
                            logger.debug("Entra al if:" + record.getValue("chargingCharacteristics").toString());

                            String dateIn = record.getValue("recordStartTime").toString();
                            Date DateOut = new SimpleDateFormat("yy-MM-dd HH:mm:ss").parse(dateIn);
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
                            String fechaTexto = formatter.format(DateOut);

                            record.setValue("recordStartTime", fechaTexto);

                            writer.write(record);
                        }

                    }
                    final WriteResult writeResult = writer.finishRecordSet();
                    writer.close();

                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                    attributes.putAll(writeResult.getAttributes());
                    session.putAllAttributes(outFlowFile, attributes);
                    session.transfer(outFlowFile, REL_SUCCESS);

                } catch (final Exception ex) {
                    ex.printStackTrace();
                    getLogger().debug("Failed process record ");
                    throw new ProcessException(ex);
                }
            } catch (final SchemaNotFoundException e) {
                throw new ProcessException("### Esquema no encontrado", e);
            } catch (final MalformedRecordException e) {
                throw new ProcessException("### Los datos entrantes no corresponden con el esquema", e);
            } finally {
                reader.close();
            }
        } catch (final Exception ex) {
            getLogger().debug("Failed to process {}; will route to failure", new Object[] {flowFile, ex});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        session.remove(flowFile);
    }


}
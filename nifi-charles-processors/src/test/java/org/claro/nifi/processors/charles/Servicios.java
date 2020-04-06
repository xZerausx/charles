package org.claro.nifi.processors.charles;

import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.TestRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


public class Servicios<runner> {


    public static void CSVReader(TestRunner runner) throws IOException, InitializationException {
        //Reader
        final CSVReader csvReader = new CSVReader();
        final String inputSchemaText = new String( Files.readAllBytes( Paths.get( "src/test/inSchema") ) );
        runner.addControllerService( "reader", csvReader );

        runner.setProperty( csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY );
        runner.setProperty( csvReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText );
        runner.setProperty( csvReader, CSVUtils.VALUE_SEPARATOR, "|" );
        runner.setProperty( csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "false" );
        runner.setProperty( csvReader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue() );
        runner.setProperty( csvReader, CSVUtils.TRAILING_DELIMITER, "false" );

        runner.enableControllerService( csvReader );
    }

    public static void  CSVWriter(TestRunner runner)  throws InitializationException, IOException {
        //Writer
        final CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        final String outputSchemaRawdata = new String( Files.readAllBytes( Paths.get( "src/test/outSchema") ) );
        runner.addControllerService( "writer", csvWriter );

        runner.setProperty( csvWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY );
        runner.setProperty( csvWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaRawdata );
        runner.setProperty( csvWriter, "Schema Write Strategy", "full-schema-attribute" );
        runner.setProperty( csvWriter, CSVUtils.INCLUDE_HEADER_LINE, "false" );
        runner.setProperty( csvWriter, CSVUtils.VALUE_SEPARATOR, "," );
        runner.setProperty( csvWriter, CSVUtils.TRAILING_DELIMITER, "false" );

        runner.enableControllerService( csvWriter );
    }


}

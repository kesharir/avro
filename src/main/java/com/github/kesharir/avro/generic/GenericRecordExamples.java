package com.github.kesharir.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordExamples {
    public static void main(String[] args) {
        // step0: define schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "  \"type\": \"record\",\n" +
                "  \"namespace\": \"com.example\",\n" +
                "  \"name\": \"Customer\",\n" +
                "  \"doc\": \"Avro Schema for our customer\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\"},\n" +
                "    {\"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\"},\n" +
                "    {\"name\": \"age\", \"type\": \"int\", \"doc\": \"Age of the Customer\"},\n" +
                "    {\"name\": \"height\", \"type\": \"float\", \"doc\": \"Height in Cms\"},\n" +
                "    {\"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight in Kilograms\"},\n" +
                "    {\"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"true if the user wants marketing emails\"}\n" +
                "  ]\n" +
                "}");
        // step1: create a generic record
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("first_name", "John");
        customerBuilder.set("last_name", "Doe");
        customerBuilder.set("age", 25);
        customerBuilder.set("height", 170f);
        customerBuilder.set("weight", 80.5f);
        customerBuilder.set("automated_email", false);
        GenericData.Record customer = customerBuilder.build();
        System.out.println(customer);

        GenericRecordBuilder customerBuilderWithDefault = new GenericRecordBuilder(schema);
        customerBuilderWithDefault.set("first_name", "John");
        customerBuilderWithDefault.set("last_name", "Doe");
        customerBuilderWithDefault.set("age", 25);
        customerBuilderWithDefault.set("height", 170f);
        customerBuilderWithDefault.set("weight", 80.5f);
        GenericData.Record customerWithDefault = customerBuilderWithDefault.build();
        System.out.println(customerWithDefault);

        // step2: write that generi record to a file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
            dataFileWriter.append(customer);
            System.out.println("Written customer-generic.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }
        // step3: read a generic record from file
        final File file = new File("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;
        try(DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            customerRead = dataFileReader.next();
            // step4: interpret as a generic record
            System.out.println("Successfully read avro file");
            System.out.println(customerRead.toString());

            // get the data from the generic record
            System.out.println("First name : " + customerRead.get("first_name"));
            // read a non existent field
            System.out.println("Non existent field : " + customerRead.get("not_here"));
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
}

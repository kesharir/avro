package com.github.kesharir.avro.specific;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExample {
    public static void main(String[] args) {
        // Step 1 : Create specific record
        Customer.Builder customerBuilder = Customer.newBuilder();
        customerBuilder.setAge(25);
        customerBuilder.setFirstName("John");
        customerBuilder.setLastName("Doe");
        customerBuilder.setHeight(170f);
        customerBuilder.setWeight(80.5f);
        customerBuilder.setAutomatedEmail(true);
        Customer customer = customerBuilder.build();
        System.out.println(customer);
        // Step 2 : Write to file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            System.out.println("Written customer-specific.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }
        // Step 3 : Read from file
        final File file = new File("customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>();
        Customer customerRead;
        try(DataFileReader<Customer> dataFileReader = new DataFileReader<>(file, datumReader)) {
            // step4: interpret as a specifc record
            customerRead = dataFileReader.next();
            System.out.println("Successfully read avro file");
            System.out.println(customerRead.toString());
            // get the data from the specifc record
            System.out.println("First name : " + customerRead.getFirstName());
            System.out.println("Last Name : " + customerRead.getLastName());
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
}

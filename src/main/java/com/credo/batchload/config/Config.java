package com.credo.batchload.config;

import com.credo.batchload.mapper.CsvMapper;
import com.credo.batchload.processor.Processor;
import com.credo.batchload.types.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SyncTaskExecutor;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class Config {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Bean
    public Job readCSVFilesJob() {
        return jobBuilderFactory
                .get("readCSVFilesJob")
                .incrementer(new RunIdIncrementer())
                .start(fullFlow())
                .build()
                .build();
    }

    @Bean
    public Flow fullFlow() {
        return new FlowBuilder<SimpleFlow>("fullFlow")
                .start(addPerson())
                .next(personSubTableFlow())
                .build();
    }

    @Bean
    public Flow personSubTableFlow() {
        return new FlowBuilder<SimpleFlow>("personSubTableFlow")
                .split(new SyncTaskExecutor())
                .add(phoneFlow(), emailFlow(), noteFlow(), addressFlow())
                // .add(addressFlow())
                .build();
    }

    @Bean
    public Flow phoneFlow() {
        return new FlowBuilder<SimpleFlow>("phoneFlow")
                .start(addPhone())
                .build();
    }

    @Bean
    public Flow emailFlow() {
        return new FlowBuilder<SimpleFlow>("emailFlow")
                .start(addEmail())
                .build();
    }

    @Bean
    public Flow noteFlow() {
        return new FlowBuilder<SimpleFlow>("noteFlow")
                .start(addNote())
                .build();
    }

    @Bean
    public Flow addressFlow() {
        return new FlowBuilder<SimpleFlow>("addressFlow")
                .start(addAddress())
                .build();
    }

    @Bean
    public Step addPerson() {
        return stepBuilderFactory.get("addPerson").<Person, Person>chunk(5)
                .reader(fileItemReader())
                .processor(new Processor("person"))
                .writer(dbWriterPerson())
                .build();
    }

    @Bean
    public Step addPhone() {
        return stepBuilderFactory.get("addPhone").<Person, Person>chunk(5)
                .reader(fileItemReader())
                .processor(new Processor("phone"))
                .writer(dbWriterPhone())
                .build();
    }

    @Bean
    public Step addEmail() {
        return stepBuilderFactory.get("addEmail").<Person, Person>chunk(5)
                .reader(fileItemReader())
                .processor(new Processor("email"))
                .writer(dbWriterEmail())
                .build();
    }

    @Bean
    public Step addNote() {
        return stepBuilderFactory.get("addNote").<Person, Person>chunk(5)
                .reader(fileItemReader())
                .processor(new Processor("note"))
                .writer(dbWriterNote())
                .build();
    }

    @Bean
    public Step addAddress() {
        return stepBuilderFactory.get("addAddress").<Person, Person>chunk(5)
                .reader(fileItemReader())
                .processor(new Processor("address"))
                .writer(dbWriterAddressHouseDetails())
                // .writer(consoleWriter())
                .build();
    }

    @Bean
    public FlatFileItemReader<Person> fileItemReader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("reader")
                .resource(new ClassPathResource("input.csv"))
                .delimited()
                .names("id", "membershipLevel", "salutation", "first", "last", "organization", "address", "city",
                        "state", "zip", "phone", "email", "unknownNumber", "lastEngagement", "isCurrent", "notes")
                .fieldSetMapper(new CsvMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public ItemWriter<Person> dbWriterPerson() {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO PERSON(id, prefix, first_name, last_name, current_member, is_head_of_house, " +
                        "is_deceased, membership_level_id) VALUES(:id, :salutation, :first, :last, :isCurrentDb, " +
                        "false, false, :membershipLevelDb)")
                .dataSource(dataSource)
                .build();
    }
    @Bean
    public ItemWriter<Person> dbWriterEmail() {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO PERSON_EMAIL(email, person_id) VALUES(:email, :id)")
                .dataSource(dataSource)
                .build();
    }
    @Bean
    public ItemWriter<Person> dbWriterPhone() {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO PERSON_PHONE(person_id, phone_number) VALUES(:id, :phone)")
                .dataSource(dataSource)
                .build();
    }
    @Bean
    public ItemWriter<Person> dbWriterNote() {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO PERSON_NOTES(person_id, notes) VALUES(:id, :notes)")
                .dataSource(dataSource)
                .build();
    }
    @Bean
    public ItemWriter<Person> dbWriterAddressHouseDetails() {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("WITH HOUSE_DETAILS_RESULT AS (INSERT INTO HOUSE_DETAILS(head_of_house_id) VALUES(:id) RETURNING id) " +
                        "INSERT INTO HOUSE_ADDRESS(house_details_id, street_address, city, state, zipcode) " +
                        "SELECT id , :address, :city, :state, :zip " +
                        "FROM HOUSE_DETAILS_RESULT")
                .dataSource(dataSource)
                .build();
    }
    @Bean
    public ItemWriter<Person> consoleWriter() {
        return list -> list.forEach(System.out::println);
    }
}

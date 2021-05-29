package com.credo.batchload.mapper;

import com.credo.batchload.types.Person;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.stereotype.Service;
import org.springframework.validation.BindException;

@Service
public class CsvMapper implements FieldSetMapper<Person> {

    @Override public Person mapFieldSet(FieldSet fieldSet) throws BindException {
        return Person.builder()
                .id(fieldSet.readInt(0))
                .membershipLevel(fieldSet.readString(1))
                .salutation(fieldSet.readString(2))
                .first(fieldSet.readString(3))
                .last(fieldSet.readString(4))
                .organization(fieldSet.readString(5))
                .address(fieldSet.readString(6))
                .city(fieldSet.readString(7))
                .state(fieldSet.readString(8))
                .zip(fieldSet.readString(9))
                .phone(fieldSet.readString(10))
                .email(fieldSet.readString(11))
                .unknownNumber(fieldSet.readString(11))
                .lastEngagement(fieldSet.readString(13))
                .isCurrent(fieldSet.readString(14))
                .notes(fieldSet.readString(15))
                .build();
    }
}

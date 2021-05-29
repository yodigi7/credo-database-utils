package com.credo.batchload.types;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Person {
    Integer id;
    String membershipLevel;
    String salutation;
    String first;
    String last;
    String organization;
    String address;
    String city;
    String state;
    String zip;
    String phone;
    String email;
    String unknownNumber;
    String lastEngagement;
    String isCurrent;
    String notes;

    Boolean isCurrentDb;
    Integer membershipLevelDb;
}

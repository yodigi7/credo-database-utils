package com.credo.batchload.processor;

import com.credo.batchload.types.Person;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class Processor implements ItemProcessor<Person, Person> {

    String filter = "";

    Map<String, Integer> membershipMapper = new HashMap<>(){{
        put("GOLD", 1);
        put("ROSE", 2);
        put("GREEN", 3);
    }};

    public Processor() {
    }

    public Processor(String filter) {
        this.filter = filter;
    }

    @Override public Person process(Person person) {
        if (person.getFirst().isBlank() && person.getLast().isBlank() && person.getSalutation().isBlank()) {
            return null;
        } else if (filter.equals("address") && person.getAddress().isBlank() && person.getCity().isBlank() && person.getState().isBlank() && person.getZip().isBlank()) {
            return null;
        } else if (filter.equals("address")) {
            person.setAddress(person.getAddress().strip());
            person.setCity(person.getCity().strip());
            person.setState(person.getState().strip());
            person.setZip(person.getZip().strip());
            if (person.getAddress().isBlank()) {
                person.setAddress(null);
            } if (person.getCity().isBlank()) {
                person.setCity(null);
            } if (person.getState().isBlank()) {
                person.setState(null);
            } if (person.getZip().isBlank()) {
                person.setZip(null);
            }
        } else if (person.getPhone().isBlank() && filter.equals("phone")) {
            return null;
        } else if (filter.equals("phone")) {
            person.setPhone(person.getPhone().strip().replaceFirst("(\\d{3})-(\\d{3})-(\\d{4})", "($1) $2-$3"));
        } else if (person.getNotes().strip().isBlank() && filter.equals("note")) {
            return null;
        } else if (person.getEmail().isBlank() && filter.equals("email")) {
            return null;
        } else if (filter.equals("person")) {
            person.setMembershipLevel(person.getMembershipLevel().strip().toUpperCase());
            if (person.getMembershipLevel().equals("#N/A") || person.getMembershipLevel().isBlank()) {
                person.setMembershipLevel(null);
            } else if (membershipMapper.containsKey(person.getMembershipLevel())) {
                person.setMembershipLevelDb(membershipMapper.get(person.getMembershipLevel()));
            }
            switch(person.getIsCurrent().strip().toUpperCase()) {
                case "Y":
                case "YES":
                    person.setIsCurrentDb(true);
                    break;
                case "N":
                case "NO":
                    person.setIsCurrentDb(false);
                    break;
                default:
                    person.setIsCurrentDb(null);
                    break;
            }
        }
        return person;
    }
}

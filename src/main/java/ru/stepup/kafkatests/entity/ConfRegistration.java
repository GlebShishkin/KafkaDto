package ru.stepup.kafkatests.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class ConfRegistration {
    String name;
    Long conferenceId;

    @Override
    public String toString() {
        return name + ',' + conferenceId;
    }
}

package com.weds.database.syncserver.utils;

import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class InstantUtil {

    private Instant instan;

    public Instant getInstan() {
        return instan;
    }

    public void setInstan(Instant instan) {
        this.instan = instan;
    }
}

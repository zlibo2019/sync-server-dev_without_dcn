package com.weds.database.syncserver.controller;

import org.springframework.scheduling.TaskScheduler;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;

public class ScheduleController {

    public static final long FIXED_RATE = 5000;

    @Resource
    private TaskScheduler taskScheduler;

    private ScheduledFuture<?> scheduledFuture;

    public void start() {
        scheduledFuture = taskScheduler.scheduleAtFixedRate(printHour(), FIXED_RATE);
    }


    private Runnable printHour() {
        return () -> System.out.println("Hello " + Instant.now().getEpochSecond());
    }

    public void stop() {
        scheduledFuture.cancel(false);
    }
}

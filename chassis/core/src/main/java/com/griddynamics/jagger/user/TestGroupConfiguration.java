package com.griddynamics.jagger.user;

import com.griddynamics.jagger.master.CompositableTask;
import com.griddynamics.jagger.master.CompositeTask;
import com.griddynamics.jagger.master.configuration.Task;
import com.griddynamics.jagger.monitoring.InfiniteDuration;
import com.griddynamics.jagger.monitoring.MonitoringTask;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestGroupConfiguration {

    private String id;
    private List<TestConfiguration> tests;
    private boolean monitoringEnabled;
    private int number;

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public boolean isMonitoringEnabled() {
        return monitoringEnabled;
    }

    public void setMonitoringEnabled(boolean monitoringEnabled) {
        this.monitoringEnabled = monitoringEnabled;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String id) {
        this.id = id;
    }

    public List<TestConfiguration> getTests() {
        return tests;
    }

    public void setTests(List<TestConfiguration> tests) {
        this.tests = tests;
    }

    public Task generate() {
        HashSet<String> names = new HashSet<String>();

        CompositeTask compositeTask = new CompositeTask();
        compositeTask.setLeading(new ArrayList<CompositableTask>());
        compositeTask.setAttendant(new ArrayList<CompositableTask>());
        compositeTask.setNumber(number);

        for (TestConfiguration testConfig : tests) {
            testConfig.setTestGroupName(id);
            testConfig.setNumber(number);
            if (!names.contains(testConfig.getName())) {
                names.add(testConfig.getName());
                //TODO figure out if it's really needed
                AtomicBoolean shutdown = new AtomicBoolean(false);
                if (testConfig.isAttendant()) {
                    compositeTask.getAttendant().add(testConfig.generate(shutdown));
                } else {
                    compositeTask.getLeading().add(testConfig.generate(shutdown));
                }
            } else {
                throw new IllegalArgumentException(String.format("Task with name '%s' already exists", testConfig.getName()));
            }
        }

        if (monitoringEnabled) {
            MonitoringTask attendantMonitoring = new MonitoringTask(number, id + " --- monitoring", id, new InfiniteDuration());
            compositeTask.getAttendant().add(attendantMonitoring);
        }
        return compositeTask;
    }
}

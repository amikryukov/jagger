package com.griddynamics.jagger.engine.e1.services.data.service;

/** Class is a model of single point id metric detailed results (values vs time)
 *
 * @details
 * MetricPlotPointEntity is used to get test results from database with use of @ref DataService
 *
 * @author
 * Gribov Kirill
 */
public class MetricPlotPointEntity {
    /** X value of detailed results - time from start of the measurement */
    private Double time;

    /** Y value of detailed results - value of the metric */
    private Double value;

    public Double getTime() {
        return time;
    }

    public void setTime(Double time) {
        this.time = time;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}

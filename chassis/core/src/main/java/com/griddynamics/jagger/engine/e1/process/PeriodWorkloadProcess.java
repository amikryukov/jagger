package com.griddynamics.jagger.engine.e1.process;

import com.griddynamics.jagger.coordinator.NodeContext;
import com.griddynamics.jagger.engine.e1.scenario.WorkloadConfiguration;
import com.griddynamics.jagger.util.TimeoutsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Workload process that perform invocation with given period.
 */
public class PeriodWorkloadProcess extends AbstractWorkloadProcess {

    private PeriodSingleTaskScheduler loopExecutor = new PeriodSingleTaskScheduler();


    Logger log = LoggerFactory.getLogger(PeriodWorkloadProcess.class);

    public PeriodWorkloadProcess(String sessionId, StartWorkloadProcess command, NodeContext context, ThreadPoolExecutor executor, TimeoutsConfiguration timeoutsConfiguration) {
        super(executor, sessionId, command, context, timeoutsConfiguration);
    }

    @Override
    protected Collection<WorkloadService> getRunningWorkloadServiceCollection() {
        return new LinkedBlockingQueue<WorkloadService>();
    }

    @Override
    protected void doStart() {
        long period = command.getScenarioContext().getWorkloadConfiguration().getPeriod();
        long delay = command.getScenarioContext().getWorkloadConfiguration().getDelay();

        // start scheduling task with given period.
        loopExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                startNewThread();
            }
        }, delay, period, TimeUnit.MILLISECONDS);
    }


    @Override
    protected void stopBeforeTerminating() {
        // do not stat new workload service
        loopExecutor.clear();
        loopExecutor.shutdown();
    }


    @Override
    protected void changeConfigurationAfterStats(WorkloadConfiguration configuration) {
        // start new period schedule according to new configuration
        loopExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                startNewThread();
            }
        }, configuration.getDelay(), configuration.getPeriod(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void changeConfigurationBeforeStats(WorkloadConfiguration configuration) {
        // stop scheduling current task
        loopExecutor.clear();
    }

    @Override
    protected WorkloadService getService(WorkloadService.WorkloadServiceBuilder serviceBuilder) {
        // return workload service that should execute 1 sample
        return serviceBuilder.buildInvokeOnDemandWorkloadService();
    }
}

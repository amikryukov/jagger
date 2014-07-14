package com.griddynamics.jagger.dbapi.model.rules;

import com.griddynamics.jagger.dbapi.parameter.DefaultMonitoringParameters;
import com.griddynamics.jagger.dbapi.parameter.GroupKey;
import com.griddynamics.jagger.util.MonitoringIdUtils;
import com.griddynamics.jagger.util.StandardMetricsNamesUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TreeViewGroupMetricsToNodeRuleProvider {

    private Map<GroupKey, DefaultMonitoringParameters[]> monitoringPlotGroups;

    public void setMonitoringPlotGroups(Map<GroupKey, DefaultMonitoringParameters[]> monitoringPlotGroups) {
        this.monitoringPlotGroups = monitoringPlotGroups;
    }

    public TreeViewGroupMetricsToNodeRule provide (Map<String,Set<String>> agentNames) {
        List<TreeViewGroupMetricsToNodeRule> result = new ArrayList<TreeViewGroupMetricsToNodeRule>();

        // Create rules to combine default monitor parameters
        if ((agentNames != null) && (agentNames.size() > 0)) {
            for(Map.Entry<GroupKey,DefaultMonitoringParameters[]> groupKeyEntry : monitoringPlotGroups.entrySet()) {
                String metricDisplayName = groupKeyEntry.getKey().getUpperName();

                // group metrics to nodes by DefaultMonitoringParameters and AgentIds
                if (agentNames.containsKey(metricDisplayName)) {
                    for (String agentId : agentNames.get(metricDisplayName)) {
                        String regex = "";
                        for (DefaultMonitoringParameters defaultMonitoringParameters : groupKeyEntry.getValue()) {
                            // not first / first time
                            if (regex.length() != 0) {
                                regex += "|";
                            }
                            else {
                                regex += "^.*(";
                            }
                            regex += defaultMonitoringParameters.getId();
                        }
                        String safeAgentId = MonitoringIdUtils.getEscapedStringForRegex(agentId);
                        regex += ").*" + safeAgentId + ".*";

                        result.add(new TreeViewGroupMetricsToNodeRule(Rule.By.ID, metricDisplayName + "_" + agentId,agentId,regex));
                    }
                }
            }
        }

        // Create rules to combine standard metrics together

        // Latency
        String regex = "^(" + StandardMetricsNamesUtil.TEMPORARY_PREFIX + StandardMetricsNamesUtil.LATENCY_ID + "|" +
                StandardMetricsNamesUtil.TEMPORARY_PREFIX + StandardMetricsNamesUtil.LATENCY_STD_DEV_ID + ")$";
        result.add(new TreeViewGroupMetricsToNodeRule(
                Rule.By.ID,
                StandardMetricsNamesUtil.LATENCY_ID + StandardMetricsNamesUtil.LATENCY_STD_DEV_ID,
                StandardMetricsNamesUtil.LATENCY,
                regex));

        // Time Latency Percentile
        regex = "^" + StandardMetricsNamesUtil.TEMPORARY_PREFIX + StandardMetricsNamesUtil.LATENCY_PERCENTILE_REGEX + "$";
        result.add(new TreeViewGroupMetricsToNodeRule(
                Rule.By.ID,
                StandardMetricsNamesUtil.TIME_LATENCY_PERCENTILE + "_id",
                StandardMetricsNamesUtil.TIME_LATENCY_PERCENTILE,
                regex));



        return TreeViewGroupMetricsToNodeRule.Composer.compose(result);
    }
}

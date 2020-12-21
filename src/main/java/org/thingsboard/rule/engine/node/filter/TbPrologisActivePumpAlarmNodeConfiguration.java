package org.thingsboard.rule.engine.node.filter;

import lombok.Data;
import org.thingsboard.rule.engine.api.NodeConfiguration;

import java.util.concurrent.TimeUnit;

@Data
public class TbPrologisActivePumpAlarmNodeConfiguration implements NodeConfiguration<TbPrologisActivePumpAlarmNodeConfiguration> {

    private TimeUnit periodTimeUnit;
    private int periodValue;
    private String telemetryKey;
    private int countOfMovement;

    @Override
    public TbPrologisActivePumpAlarmNodeConfiguration defaultConfiguration() {
        TbPrologisActivePumpAlarmNodeConfiguration configuration = new TbPrologisActivePumpAlarmNodeConfiguration();
        configuration.setPeriodValue(5);
        configuration.setCountOfMovement(1);
        configuration.setTelemetryKey("PRESENCE_MOVE_COUNT_VALUE");
        configuration.setPeriodTimeUnit(TimeUnit.MINUTES);
        return configuration;
    }
}
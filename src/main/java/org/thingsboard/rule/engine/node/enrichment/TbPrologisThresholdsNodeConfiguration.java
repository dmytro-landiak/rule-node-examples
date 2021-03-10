package org.thingsboard.rule.engine.node.enrichment;

import lombok.Data;
import org.thingsboard.rule.engine.api.NodeConfiguration;

import java.util.HashSet;
import java.util.Set;

@Data
public class TbPrologisThresholdsNodeConfiguration implements NodeConfiguration<TbPrologisThresholdsNodeConfiguration> {

    private Set<String> telemetryKeysNames;

    @Override
    public TbPrologisThresholdsNodeConfiguration defaultConfiguration() {
        TbPrologisThresholdsNodeConfiguration configuration = new TbPrologisThresholdsNodeConfiguration();
        configuration.setTelemetryKeysNames(new HashSet<>());
        return configuration;
    }
}

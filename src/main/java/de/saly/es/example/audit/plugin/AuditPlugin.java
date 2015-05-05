/*
Copyright 2015 Hendrik Saly

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package de.saly.es.example.audit.plugin;

import java.util.Collection;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

import de.saly.es.example.audit.service.AuditService;

public class AuditPlugin extends AbstractPlugin {

    private final Settings settings;
    private final boolean clientMode;

    public AuditPlugin(final Settings settings) {
        this.settings = settings;
        this.clientMode = clientMode(settings);
    }

    public static boolean clientMode(final Settings settings) {
        return !"node".equals(settings.get("client.type"));
    }

    @Override
    public String name() {
        return "AuditPlugin";
    }

    @Override
    public String description() {
        return "This is the description for the AuditPlugin";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        return ImmutableList.<Class<? extends Module>> of(AuditModule.class);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {

        if (!clientMode) {
            return ImmutableList.<Class<? extends LifecycleComponent>> of(AuditService.class);
        }

        return ImmutableList.<Class<? extends LifecycleComponent>> of();
    }

}

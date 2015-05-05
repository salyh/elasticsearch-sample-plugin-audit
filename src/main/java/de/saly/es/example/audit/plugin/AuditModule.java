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

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;

public class AuditModule extends AbstractModule implements SpawnModules {

    private final Settings settings;
    private final boolean clientMode;

    public AuditModule(final Settings settings) {
        this.settings = settings;
        this.clientMode = AuditPlugin.clientMode(settings);
    }

    @Override
    protected void configure() {

    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        if (!clientMode) {
            return ImmutableList.<Module> of(new AuditActionModule(), new AuditRestModule());
        }

        return ImmutableList.<Module> of(new AuditActionModule());
    }

}

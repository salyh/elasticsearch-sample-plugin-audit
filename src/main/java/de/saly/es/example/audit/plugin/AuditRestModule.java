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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.rest.RestModule;

import de.saly.es.example.audit.rest.AuditRestAction;

public class AuditRestModule extends AbstractModule implements PreProcessModule {

    @Override
    protected void configure() {

    }

    @Override
    public void processModule(final Module module) {
        if ((module instanceof RestModule)) {
            ((RestModule) module).addRestAction(AuditRestAction.class);
        }
    }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.core.event;

import com.google.common.base.Joiner;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.analysis.Stream;
import org.apache.skywalking.oap.server.core.analysis.record.Record;
import org.apache.skywalking.oap.server.core.analysis.worker.RecordStreamProcessor;
import org.apache.skywalking.oap.server.core.source.ScopeDeclaration;
import org.apache.skywalking.oap.server.core.storage.StorageBuilder;
import org.apache.skywalking.oap.server.core.storage.annotation.Column;

import static org.apache.skywalking.oap.server.core.source.DefaultScopeDefine.EVENT;

@Getter
@Setter
@ScopeDeclaration(id = EVENT, name = "Event")
@Stream(name = EventRecord.INDEX_NAME, scopeId = EVENT, builder = EventRecord.Builder.class, processor = RecordStreamProcessor.class)
public class EventRecord extends Record {

    public static final String INDEX_NAME = "event_record";

    public static final String UUID = "uuid";

    public static final String SERVICE = "service";

    public static final String SERVICE_INSTANCE = "service_instance";

    public static final String ENDPOINT = "endpoint";

    public static final String NAME = "name";

    public static final String TYPE = "type";

    public static final String MESSAGE = "message";

    public static final String PARAMETERS = "parameters";

    public static final String START_TIME = "start_time";

    public static final String END_TIME = "end_time";

    @Override
    public String id() {
        return Joiner.on(Const.ID_CONNECTOR)
                     .skipNulls()
                     .join(getUuid(), getName(), getStartTime(), getService(), getServiceInstance());
    }

    @Column(columnName = UUID)
    private String uuid;

    @Column(columnName = SERVICE)
    private String service;

    @Column(columnName = SERVICE_INSTANCE)
    private String serviceInstance;

    @Column(columnName = ENDPOINT)
    private String endpoint;

    @Column(columnName = NAME)
    private String name;

    @Column(columnName = TYPE)
    private String type;

    @Column(columnName = MESSAGE)
    private String message;

    @Column(columnName = PARAMETERS, storageOnly = true)
    private String parameters;

    @Column(columnName = START_TIME)
    private long startTime;

    @Column(columnName = END_TIME)
    private long endTime;

    public static class Builder implements StorageBuilder<EventRecord> {
        @Override
        public Map<String, Object> data2Map(EventRecord storageData) {
            Map<String, Object> map = new HashMap<>();
            map.put(UUID, storageData.getUuid());
            map.put(SERVICE, storageData.getService());
            map.put(SERVICE_INSTANCE, storageData.getServiceInstance());
            map.put(ENDPOINT, storageData.getEndpoint());
            map.put(NAME, storageData.getName());
            map.put(TYPE, storageData.getType());
            map.put(MESSAGE, storageData.getMessage());
            map.put(PARAMETERS, storageData.getParameters());
            map.put(START_TIME, storageData.getStartTime());
            map.put(END_TIME, storageData.getEndTime());
            return map;
        }

        @Override
        public EventRecord map2Data(Map<String, Object> dbMap) {
            EventRecord record = new EventRecord();
            record.setUuid((String) dbMap.get(UUID));
            record.setService((String) dbMap.get(SERVICE));
            record.setServiceInstance((String) dbMap.get(SERVICE_INSTANCE));
            record.setEndpoint((String) dbMap.get(ENDPOINT));
            record.setName((String) dbMap.get(NAME));
            record.setType((String) dbMap.get(TYPE));
            record.setMessage((String) dbMap.get(MESSAGE));
            record.setParameters((String) dbMap.get(PARAMETERS));
            record.setStartTime(((Number) dbMap.get(START_TIME)).longValue());
            record.setEndTime(((Number) dbMap.get(END_TIME)).longValue());
            return record;
        }
    }
}

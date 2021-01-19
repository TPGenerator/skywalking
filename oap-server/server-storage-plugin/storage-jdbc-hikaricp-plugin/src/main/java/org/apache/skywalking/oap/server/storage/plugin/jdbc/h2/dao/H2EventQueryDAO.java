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

package org.apache.skywalking.oap.server.storage.plugin.jdbc.h2.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.event.EventRecord;
import org.apache.skywalking.oap.server.core.query.type.TimeRange;
import org.apache.skywalking.oap.server.core.query.type.event.Event;
import org.apache.skywalking.oap.server.core.query.type.event.EventQueryCondition;
import org.apache.skywalking.oap.server.core.query.type.event.Events;
import org.apache.skywalking.oap.server.core.query.type.event.Source;
import org.apache.skywalking.oap.server.core.query.type.event.Type;
import org.apache.skywalking.oap.server.core.storage.query.IEventQueryDAO;
import org.apache.skywalking.oap.server.library.client.jdbc.hikaricp.JDBCHikariCPClient;

import static com.google.common.base.Strings.isNullOrEmpty;

@Slf4j
@RequiredArgsConstructor
public class H2EventQueryDAO implements IEventQueryDAO {
    private final JDBCHikariCPClient client;

    @Override
    public Events queryEvents(final EventQueryCondition condition) throws Exception {
        List<String> conditions = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();

        if (!isNullOrEmpty(condition.getUuid())) {
            conditions.add(EventRecord.UUID + "=?");
            parameters.add(condition.getUuid());
        }

        final Source source = condition.getSource();
        if (source != null) {
            if (!isNullOrEmpty(source.getService())) {
                conditions.add(EventRecord.SERVICE + "=?");
                parameters.add(source.getService());
            }
            if (!isNullOrEmpty(source.getServiceInstance())) {
                conditions.add(EventRecord.SERVICE_INSTANCE + "=?");
                parameters.add(source.getServiceInstance());
            }
            if (!isNullOrEmpty(source.getEndpoint())) {
                conditions.add(EventRecord.ENDPOINT + "=?");
                parameters.add(source.getEndpoint());
            }
        }

        if (!isNullOrEmpty(condition.getName())) {
            conditions.add(EventRecord.NAME + "=?");
            parameters.add(condition.getName());
        }

        if (condition.getType() != null) {
            conditions.add(EventRecord.TYPE + "=?");
            parameters.add(condition.getType().name());
        }

        final TimeRange startTime = condition.getStartTime();
        if (startTime != null) {
            if (startTime.getStart() != null && startTime.getStart() > 0) {
                conditions.add(EventRecord.START_TIME + ">?");
                parameters.add(startTime.getStart());
            }
            if (startTime.getEnd() != null && startTime.getEnd() > 0) {
                conditions.add(EventRecord.START_TIME + "<?");
                parameters.add(startTime.getEnd());
            }
        }

        final TimeRange endTime = condition.getEndTime();
        if (endTime != null) {
            if (endTime.getStart() != null && endTime.getStart() > 0) {
                conditions.add(EventRecord.END_TIME + ">?");
                parameters.add(endTime.getStart());
            }
            if (endTime.getEnd() != null && endTime.getEnd() > 0) {
                conditions.add(EventRecord.END_TIME + "<?");
                parameters.add(endTime.getEnd());
            }
        }

        final String whereClause = conditions.isEmpty() ? "" : conditions.stream().collect(Collectors.joining(" and ", " where ", ""));

        final Events result = new Events();

        try (final Connection connection = client.getConnection()) {
            String sql = "select count(1) total from (select 1 from " + EventRecord.INDEX_NAME + whereClause + " )";
            if (log.isDebugEnabled()) {
                log.debug("Count SQL: {}, parameters: {}", sql, parameters);
            }
            try (final ResultSet resultSet = client.executeQuery(connection, sql, parameters.toArray())) {
                if (!resultSet.next()) {
                    return result;
                }
                result.setTotal(resultSet.getInt("total"));
            }

            int limit = DEFAULT_SIZE;
            if (condition.getSize() > 0) {
                limit = condition.getSize();
            }
            limit = Math.min(MAX_SIZE, limit);

            sql = "select * from " + EventRecord.INDEX_NAME + whereClause + " limit " + limit;
            if (log.isDebugEnabled()) {
                log.debug("Query SQL: {}, parameters: {}", sql, parameters);
            }
            try (final ResultSet resultSet = client.executeQuery(connection, sql, parameters.toArray())) {
                while (resultSet.next()) {
                    final Event event = new Event();

                    event.setUuid(resultSet.getString(EventRecord.UUID));

                    final String service = resultSet.getString(EventRecord.SERVICE);
                    final String serviceInstance = resultSet.getString(EventRecord.SERVICE_INSTANCE);
                    final String endpoint = resultSet.getString(EventRecord.ENDPOINT);

                    event.setSource(new Source(service, serviceInstance, endpoint));
                    event.setName(resultSet.getString(EventRecord.NAME));
                    event.setType(Type.parse(resultSet.getString(EventRecord.TYPE)));
                    event.setMessage(resultSet.getString(EventRecord.MESSAGE));
                    event.setParameters(resultSet.getString(EventRecord.PARAMETERS));
                    event.setStartTime(resultSet.getLong(EventRecord.START_TIME));
                    event.setEndTime(resultSet.getLong(EventRecord.END_TIME));

                    result.getEvents().add(event);
                }
            }
        }

        return result;
    }
}

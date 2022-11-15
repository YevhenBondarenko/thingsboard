/**
 * Copyright © 2016-2022 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.dao.nosql;

import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.env.Environment;
import org.springframework.test.util.ReflectionTestUtils;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.BaseDeleteTsKvQuery;
import org.thingsboard.server.dao.cassandra.CassandraCluster;
import org.thingsboard.server.dao.cassandra.guava.GuavaSession;
import org.thingsboard.server.dao.model.ModelConstants;
import org.thingsboard.server.dao.timeseries.CassandraBaseTimeseriesDao;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CassandraBaseTimeseriesDaoTest {

    @Spy
    private CassandraBaseTimeseriesDao cassandraBaseTimeseriesDao;

    @Mock
    private Environment environment;

    @Mock
    private CassandraCluster cluster;

    @Mock
    private GuavaSession session;

    @Before
    public void setUp() throws Exception {
        ReflectionTestUtils.setField(cassandraBaseTimeseriesDao, "partitioning", "MONTHS");
        ReflectionTestUtils.setField(cassandraBaseTimeseriesDao, "environment", environment);
        ReflectionTestUtils.setField(cassandraBaseTimeseriesDao, "cluster", cluster);
        when(cluster.getSession()).thenReturn(session);
        willReturn(new TbResultSetFuture(SettableFuture.create())).given(cassandraBaseTimeseriesDao).executeAsyncRead(any(), any());
    }

    @Test
    public void testRemovePartition() throws Exception {
        cassandraBaseTimeseriesDao.init();

        UUID id = UUID.randomUUID();
        TenantId tenantId = TenantId.fromUUID(id);

        long startTs = LocalDateTime.parse("2022-11-20T00:00").toInstant(ZoneOffset.UTC).toEpochMilli();
        long endTs = LocalDateTime.parse("2022-12-20T00:00").toInstant(ZoneOffset.UTC).toEpochMilli();

        cassandraBaseTimeseriesDao.removePartition(tenantId, tenantId, new BaseDeleteTsKvQuery("test", startTs, endTs));
        verify(cassandraBaseTimeseriesDao, times(0)).executeAsyncRead(any(TenantId.class), any(Statement.class));

        startTs = LocalDateTime.parse("2022-11-01T00:00").toInstant(ZoneOffset.UTC).toEpochMilli();
        endTs = LocalDateTime.parse("2022-11-20T00:00").toInstant(ZoneOffset.UTC).toEpochMilli();

        cassandraBaseTimeseriesDao.removePartition(tenantId, tenantId, new BaseDeleteTsKvQuery("test", startTs, endTs));
        verify(cassandraBaseTimeseriesDao, times(0)).executeAsyncRead(any(TenantId.class), any(Statement.class));

        startTs = LocalDateTime.parse("2022-10-20T00:00").toInstant(ZoneOffset.UTC).toEpochMilli();
        endTs = LocalDateTime.parse("2022-12-20T00:00").toInstant(ZoneOffset.UTC).toEpochMilli();

        cassandraBaseTimeseriesDao.removePartition(tenantId, tenantId, new BaseDeleteTsKvQuery("test", startTs, endTs));

        Select select = QueryBuilder.selectFrom(ModelConstants.TS_KV_PARTITIONS_CF).column(ModelConstants.PARTITION_COLUMN)
                .whereColumn(ModelConstants.ENTITY_TYPE_COLUMN).isEqualTo(literal("TENANT"))
                .whereColumn(ModelConstants.ENTITY_ID_COLUMN).isEqualTo(literal(tenantId.getId()))
                .whereColumn(ModelConstants.KEY_COLUMN).isEqualTo(literal("test"))
                .whereColumn(ModelConstants.PARTITION_COLUMN)
                .isGreaterThanOrEqualTo(literal(LocalDateTime.parse("2022-11-01T00:00").toInstant(ZoneOffset.UTC).toEpochMilli()))
                .whereColumn(ModelConstants.PARTITION_COLUMN)
                .isLessThanOrEqualTo(literal(LocalDateTime.parse("2022-12-01T00:00").toInstant(ZoneOffset.UTC).toEpochMilli()));

        verify(cassandraBaseTimeseriesDao, times(1)).executeAsyncRead(eq(tenantId), eq(select.build()));
    }

}

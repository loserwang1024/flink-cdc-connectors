package com.ververica.cdc.connectors.postgres.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

/**
 * todo: 修正. The {@link SourceEvent} that {@link
 * com.ververica.cdc.connectors.postgres.source.enumerator.PostgresSourceEnumerator} broadcasts to
 * {@link com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader} to tell the
 * source reader to update the binlog split after newly added table snapshot splits finished.
 */
public class SyncAssignStatusAck implements SourceEvent {
    private static final long serialVersionUID = 1L;

    public SyncAssignStatusAck() {}
}

package com.ververica.cdc.connectors.postgres.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

/**
 * The {@link SourceEvent} that {@link com.ververica.cdc.connectors.postgres.source.enumerator.PostgresSourceEnumerator} broadcasts to {@link
 * com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader} to tell the source reader to update the binlog split after newly added table
 * snapshot splits finished.
 */
public class SyncAssignStatus implements SourceEvent {
    int statusCode;

    public SyncAssignStatus(int statusCode) {
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }
}

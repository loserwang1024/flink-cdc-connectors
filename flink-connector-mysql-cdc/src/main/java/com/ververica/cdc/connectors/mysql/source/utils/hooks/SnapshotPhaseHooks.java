package com.ververica.cdc.connectors.mysql.source.utils.hooks;

import javax.annotation.Nullable;

import java.io.Serializable;

public class SnapshotPhaseHooks implements Serializable {
    private static final long serialVersionUID = 1L;

    private SnapshotPhaseHook preLowWatermarkAction;
    private SnapshotPhaseHook postLowWatermarkAction;
    private SnapshotPhaseHook preHighWatermarkAction;
    private SnapshotPhaseHook postHighWatermarkAction;

    public void setPreHighWatermarkAction(SnapshotPhaseHook preHighWatermarkAction) {
        this.preHighWatermarkAction = preHighWatermarkAction;
    }

    public void setPostHighWatermarkAction(SnapshotPhaseHook postHighWatermarkAction) {
        this.postHighWatermarkAction = postHighWatermarkAction;
    }

    public void setPreLowWatermarkAction(SnapshotPhaseHook preLowWatermarkAction) {
        this.preLowWatermarkAction = preLowWatermarkAction;
    }

    public void setPostLowWatermarkAction(SnapshotPhaseHook postLowWatermarkAction) {
        this.postLowWatermarkAction = postLowWatermarkAction;
    }

    @Nullable
    public SnapshotPhaseHook getPreHighWatermarkAction() {
        return preHighWatermarkAction;
    }

    @Nullable
    public SnapshotPhaseHook getPostHighWatermarkAction() {
        return postHighWatermarkAction;
    }

    @Nullable
    public SnapshotPhaseHook getPreLowWatermarkAction() {
        return preLowWatermarkAction;
    }

    @Nullable
    public SnapshotPhaseHook getPostLowWatermarkAction() {
        return postLowWatermarkAction;
    }

    public static SnapshotPhaseHooks empty() {
        return new SnapshotPhaseHooks();
    }
}

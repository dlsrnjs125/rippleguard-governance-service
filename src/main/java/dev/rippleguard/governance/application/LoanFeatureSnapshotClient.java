package dev.rippleguard.governance.application;

import java.util.UUID;

public interface LoanFeatureSnapshotClient {
    Phase2FeatureSnapshot getByReference(UUID applicationId, String snapshotVersion);
}

package datadependency

// DataDependency-related shared constants used across controller and plugins.
// Centralizing these avoids duplication and keeps annotation/finalizer keys consistent.
const (
    // Annotation indicating placement has been injected into ResourceBinding by data dependency controller
    PlacementInjectedAnnotation = "datadependency.volcano.sh/placement-injected"

    // Annotation recording excluded clusters computed from data locality
    ExcludedClustersAnnotation = "datadependency.volcano.sh/excluded-clusters"

    // Annotation hint from dispatcher to controller about associated ResourceBindings
    AssociatedRBsAnnotation = "datadependency.volcano.sh/associated-rbs"

    // Finalizer for DataSource to ensure proper cleanup of claim references
    DataSourceFinalizer = "datadependency.volcano.sh/datasource-finalizer"

    // Finalizer for DataSourceClaim lifecycle management
    DataSourceClaimFinalizer = "datadependency.volcano.sh/finalizer"

    // Shared index name for linking DSCs and RBs via WorkloadRef
    WorkloadRefIndex = "workloadRef"
)

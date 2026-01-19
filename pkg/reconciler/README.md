# Reconciler Framework

This directory contains reconcilers built with [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime). Reconcilers share a single controller-runtime Manager instance for better resource efficiency.

## Architecture

```
reconciler.go           # Shared Manager and framework setup
scheme/                 # Registration mechanism
options/                # CLI flags support
hyperjob/              # Example: HyperJob reconciler
```

The reconciler framework registers itself as a volcano controller named `reconciler`. You can control which specific reconcilers are enabled via the `--reconcilers` flag.

## Adding a New Reconciler

Let's say you want to add a reconciler for a CRD called `MyJob`.

### Step 1: Create Directory Structure

```bash
mkdir -p pkg/reconciler/myjob
```

### Step 2: Implement the Reconciler

Create `myjob/myjob_controller.go`:

```go
package myjob

import (
    "context"
    
    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/client-go/tools/record"
    ctrl "sigs.k8s.io/controller-runtime"
    "sigs.k8s.io/controller-runtime/pkg/client"
    
    myapiv1alpha1 "volcano.sh/apis/pkg/apis/myapi/v1alpha1"
    "volcano.sh/volcano-global/pkg/reconciler/scheme"
)

const (
    ReconcilerName = "myjob"  // Used for registration and CLI
)

func init() {
    scheme.ReconcilerInitializers[ReconcilerName] = InitMyJobController
}

type MyJobController struct {
    client.Client
    Scheme   *runtime.Scheme
    Recorder record.EventRecorder
}

func InitMyJobController(mgr ctrl.Manager) error {
    recorder := mgr.GetEventRecorderFor(ReconcilerName)
    reconciler := NewMyJobController(mgr.GetClient(), mgr.GetScheme(), recorder)
    return reconciler.SetupWithManager(mgr)
}

func NewMyJobController(client client.Client, scheme *runtime.Scheme, recorder record.EventRecorder) *MyJobController {
    return &MyJobController{
        Client:   client,
        Scheme:   scheme,
        Recorder: recorder,
    }
}

func (c *MyJobController) SetupWithManager(mgr ctrl.Manager) error {
    return ctrl.NewControllerManagedBy(mgr).
        For(&myapiv1alpha1.MyJob{}).
        Complete(c)
}

func (c *MyJobController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    log := ctrl.LoggerFrom(ctx)
    
    myJob := &myapiv1alpha1.MyJob{}
    if err := c.Get(ctx, req.NamespacedName, myJob); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }
    
    // Your reconciliation logic here
    log.V(4).Info("Reconciling MyJob", "name", myJob.Name)
    
    return ctrl.Result{}, nil
}
```

### Step 3: Register CRD Scheme

If you're using a new CRD, register its scheme in `reconciler.go`:

```go
import (
    myapiv1alpha1 "volcano.sh/apis/pkg/apis/myapi/v1alpha1"
)

func init() {
    utilruntime.Must(myapiv1alpha1.AddToScheme(scheme))
    // ... existing schemes
}
```

### Step 4: Import in reconciler.go

Add import to make sure the init() function runs:

```go
import (
    // ... existing imports
    _ "volcano.sh/volcano-global/pkg/reconciler/myjob"
)
```

### Step 5: Update Available Reconcilers

Update `options/options.go` to document the new reconciler:

```go
func (o *Options) AddFlags(fs *pflag.FlagSet) {
    fs.StringSliceVar(&o.EnabledReconcilers, "reconcilers", o.EnabledReconcilers,
        "Comma-separated list of reconcilers to enable. Use '*' to enable all. "+
            "Current Available reconcilers: hyperjob, myjob")
}
```

That's it! Your reconciler will be automatically registered and available.

## Usage

```bash
# Enable all reconcilers (default)
--controllers=reconciler

# Enable specific reconcilers
--controllers=reconciler --reconcilers=hyperjob,myjob

# Enable only one reconciler
--controllers=reconciler --reconcilers=myjob
```

## Key Concepts

### Reconcile Function

The `Reconcile` function is called whenever watched resources change. It must be idempotent - calling it multiple times should produce the same result.

### Watch Configuration

Use `SetupWithManager` to configure what triggers reconciliation:

```go
func (c *MyJobController) SetupWithManager(mgr ctrl.Manager) error {
    return ctrl.NewControllerManagedBy(mgr).
        For(&myapiv1alpha1.MyJob{}).           // Primary resource
        Owns(&corev1.Pod{}).                    // Child resources
        Complete(c)
}
```

- `For()` watches the primary resource
- `Owns()` watches child resources created by this controller

### Owner References

Use owner references to establish parent-child relationships:

```go
import "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

pod := &corev1.Pod{...}
if err := controllerutil.SetControllerReference(parent, pod, c.Scheme); err != nil {
    return err
}
return c.Create(ctx, pod)
```

This enables automatic garbage collection when the parent is deleted.

### Status Updates

Always use the Status subresource for status updates:

```go
// Wrong - updates entire object
c.Update(ctx, myJob)

// Correct - only updates status
c.Status().Update(ctx, myJob)
```

## Testing

You can test reconcilers using fake clients:

```go
func TestReconcile(t *testing.T) {
    scheme := runtime.NewScheme()
    _ = myapiv1alpha1.AddToScheme(scheme)
    
    client := fake.NewClientBuilder().
        WithScheme(scheme).
        WithObjects(/* initial objects */).
        Build()
    
    controller := NewMyJobController(client, scheme, nil)
    
    result, err := controller.Reconcile(context.Background(), ctrl.Request{
        NamespacedName: types.NamespacedName{
            Name:      "test",
            Namespace: "default",
        },
    })
    
    assert.NoError(t, err)
}
```

See `hyperjob/hyperjob_controller_test.go` for more complete examples.

## References

- [HyperJob Controller](hyperjob/hyperjob_controller.go) - Complete working example
- [controller-runtime docs](https://pkg.go.dev/sigs.k8s.io/controller-runtime)

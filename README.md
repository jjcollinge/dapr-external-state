# External Dapr State Store
This is an experiment to see whether or not we can easily build an external Dapr state store using the existing components.
The code in this repo was hacked together in a couple of hours and is only meant to demonstrate a potential approach to supporting external
state stores in Dapr.

This repo relies on the following forks of Dapr:
* https://github.com/jjcollinge/components-contrib/tree/ext-state
* https://github.com/jjcollinge/dapr/tree/ext-state

# gRPC API
This program hosts a gRPC API for the Dapr State Store components based on [this `proto` definition](https://github.com/jjcollinge/components-contrib/blob/ext-state/state/proto/v1/store.proto).
Dapr also uses an [`external` state store provider](https://github.com/jjcollinge/components-contrib/tree/ext-state/state/external) which is loaded [here](https://github.com/jjcollinge/dapr/blob/4b442ec9de478ce344d9ac682959dfdde0c0e997/cmd/daprd/main.go#L240).
This service then maps the protobuf messages to the Dapr state store component types and calls a local state store.
This service is written in Go so that it can leverage the existing state stores. However, this gRPC service could just as easily be written in a different
language and call custom state store implementations.

# Usage
To run the external state store service. Run the command below:

```
go run main.go
```

> If you wish to override the port that the service listens on, set the `EXT_SS_PORT` environment variable or edit the `launch.json` file.

### Components

To use the `external` state store with Dapr you must define a state store component as normal.
The metadata will be sent to the external state store but we must first know where that external state store is hosted.
Therefore, we can simply add an additional property `externalAddress` to our component metadata that points to the address of the external state store you are running. The rest of the metadata should be aligned to the concrete state store implementation you are using (in this case Redis).

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: statestore
  namespace: statestore
spec:
  type: state.external
  version: v1
  metadata:
  - name: externalAddress <--- This is new
    value: localhost:9191
  - name: redisHost
    value: localhost:6379
```

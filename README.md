# External Dapr State Store
This is an experiment to see whether or not we can easily build an external Dapr state store using the existing components.
The code in this repo was hacked together in a couple of hours and is only meant to demonstrate a potential approach to supporting external
state stores in Dapr.

# gRPC API
This program hosts a gRPC API for the Dapr State Store components based on [this `proto` in my `dapr` fork](https://github.com/jjcollinge/components-contrib/blob/ext-state/state/proto/v1/store.proto).
Dapr also uses an [`external` state store provider](https://github.com/jjcollinge/components-contrib/tree/ext-state/state/external) from my fork which is loaded [here](https://github.com/jjcollinge/dapr/blob/4b442ec9de478ce344d9ac682959dfdde0c0e997/cmd/daprd/main.go#L240).
This service then maps the protobuf messages to the Dapr state store component types and calls a local state store.
This service is written in Go so that it can leverage the existing state stores. However, this gRPC service could just as easily be written in a different
language and call custom state store implementations.
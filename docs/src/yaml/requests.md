# gRPC requests

Every request names an endpoint and a gRPC method description:

```yaml
- request:
    name: submit
    url: grpcs://api.example.com:443
    timeout: 10s
    headers: { x-tenant: loadtest }
    grpc:
      proto_files: [protos/transactions.proto]
      proto_includes: [protos/includes]
      service: payments.Transactions
      method: Submit
      message: { account: "${data.users.account}", amount: 10 }
      metadata: { authorization: "Bearer ${secrets.token}" }
      channel_pool_size: 8
      transport: raw
    checks:
      - { type: status, equals: 0 }
      - { type: protobuf_field, field: receipt.code, equals: 1 }
```

Use `reflection: true` instead of `proto_files` when reflection is available.
For client-streaming and bidi methods, set `messages` to an array. The method
descriptor determines the call shape, so the same block handles unary and both
stream directions.

String leaves in `message`, `messages`, headers, and metadata support template
interpolation. Response JSON may be checked or extracted; protobuf-field checks
avoid materializing the full JSON response.

`channel_pool_size` shares a fixed pool across VUs. `transport` is `channel`
(tonic's buffered channel) or `raw` (direct hyper HTTP/2).

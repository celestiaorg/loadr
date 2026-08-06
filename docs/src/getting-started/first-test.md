# Your first gRPC test

```yaml
name: hello
scenarios:
  default:
    executor: constant-vus
    vus: 5
    duration: 30s
    flow:
      - request:
          url: grpc://localhost:50051
          grpc:
            proto_files: [protos/helloworld.proto]
            service: helloworld.Greeter
            method: SayHello
            message: { name: "vu-${vu}" }
          checks: [{ type: status, equals: 0 }]
thresholds:
  grpc_req_duration: ["p(95)<100"]
```

Validate and run it:

```bash
loadr validate test.yaml
loadr run test.yaml
```

Use `reflection: true` instead of `proto_files` when the target exposes gRPC
server reflection.

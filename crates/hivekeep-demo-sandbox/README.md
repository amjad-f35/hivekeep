# hivekeep-demo-sandbox

The sandbox demo. `sandbox` runs inside an Actor and executes the commands
it receives on `POST /process`. `sandbox-client` resumes the Actor, gives
you a `sandbox>` prompt through the gate, and suspends the Actor when you
leave. The demo does no authentication: never expose it to untrusted
callers. Walkthrough: [demos/sandbox](../../demos/sandbox/README.md).

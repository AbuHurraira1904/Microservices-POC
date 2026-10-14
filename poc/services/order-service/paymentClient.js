// =============================================================================
// PAYMENT CLIENT  (Order service)
// =============================================================================
// How the Order service calls the Payment service.
//
// Why gRPC (and not REST) here
//   Payment is an INTERNAL service that only our own services call. gRPC suits
//   that: the call's shape is defined in a shared contract file
//   (proto/payment.proto) that both sides load, the messages are compact, and
//   mistakes in field names or types are caught early. A real card processor,
//   by contrast, is reached over REST/webhooks, which is why Payment is the
//   one place where we can choose gRPC freely.
//
// Why mTLS ("mutual TLS")
//   Ordinary HTTPS lets the client check the server's identity. In mTLS BOTH
//   sides show a certificate signed by our own certificate authority (CA):
//     - ca.crt                   the CA's public certificate, used to verify the
//                                other side
//     - order.crt + order.key    this service's own identity (certificate and
//                                private key)
//   Payment checks the Order certificate too, so a stranger who reaches the
//   Payment port still cannot call it. The traffic is also encrypted.
//
// Decisions made here, and why
//   - A deadline on every call (3 seconds). Without one, a hung Payment service
//     would freeze the customer's request, and the requests behind it, forever.
//   - The call is wrapped in a Promise so the caller can use `await`.
//   - Certificates are read from files; their folder can be moved with
//     CERTS_DIR. The certs/ folder is in .gitignore: keys and certificates must
//     never be committed.
//
// PRODUCTION CHANGES (what a full system would do differently)
//   - Certificates would come from a managed system (cert-manager, a service
//     mesh, or a cloud CA), be short-lived, and rotate automatically. Here they
//     are long-lived files made by hand.
//   - Send an idempotency key with each charge (a unique id per payment
//     attempt). Then a retry after a timeout can never charge the customer
//     twice. Without it, a timeout is ambiguous (the charge may have succeeded)
//     and this client must not simply retry.
//   - Add retries with backoff for safe cases, and a circuit breaker that stops
//     calling Payment for a while when it keeps failing.
//   - Send the amount as whole cents (an integer), not a decimal number.
//   - Add health checks and load balancing across several Payment instances
//     (today there is exactly one, at one fixed address).
// =============================================================================

const path = require('path');
const fs = require('fs');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

// The shared contract. `keepCase: true` keeps field names such as order_id
// exactly as written in the file instead of renaming them to orderId.
const PROTO_PATH = path.join(__dirname, '../../proto/payment.proto');
const CERTS_DIR = process.env.CERTS_DIR || path.join(__dirname, '../../certs');
const readCert = (name) => fs.readFileSync(path.join(CERTS_DIR, name));

const def = protoLoader.loadSync(PROTO_PATH, { keepCase: true, longs: String, defaults: true });
const { PaymentService } = grpc.loadPackageDefinition(def).payment;

// The mTLS identity: the CA to trust, plus this service's own key and
// certificate to show to Payment.
const credentials = grpc.credentials.createSsl(
  readCert('ca.crt'),
  readCert('order.key'),
  readCert('order.crt')
);

// One client for the whole life of the service. gRPC keeps its connection
// open and reuses it for every call.
const client = new PaymentService(process.env.PAYMENT_SERVICE_ADDR, credentials);

// Charges the customer. Resolves with Payment's answer
// ({ approved, transaction_id, reason }), or rejects on a network error, a
// refused certificate, or when the 3-second deadline passes (the error code is
// then DEADLINE_EXCEEDED, which index.js turns into HTTP 504).
// A declined payment is NOT an error: it resolves with approved = false.
function charge(orderId, amount, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    client.Charge(
      { order_id: orderId, amount },
      { deadline: Date.now() + timeoutMs },
      (err, res) => (err ? reject(err) : resolve(res))
    );
  });
}

module.exports = { charge };

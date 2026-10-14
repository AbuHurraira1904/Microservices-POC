// =============================================================================
// PAYMENT TEST CLIENT  (a manual check, not part of the running system)
// =============================================================================
// A tiny stand-alone script for trying the Payment service by hand, without
// starting the Order service:
//
//     node test-client.js        (with the Payment service already running)
//
// It sends one charge (order 1, amount 49.99) and prints the answer: either
// { approved, transaction_id, reason } or a gRPC error. Useful to check that
// the mTLS certificates work: with a missing or wrong client certificate the
// call fails with a connection error instead of an answer.
//
// It uses the same client certificate as the Order service (order.crt and
// order.key), so Payment accepts it as a legitimate caller.
//
// PRODUCTION CHANGES (what a full system would do differently)
//   - Replace this script with automated tests that run in the build
//     pipeline: a real test framework, assertions on the result, and a test
//     Payment service started for the test.
//   - Take the address and certificate folder from settings, not constants.
// =============================================================================

const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

// Load the same contract (payment.proto) that the service and the Order
// client use, so all three agree on the shape of the call.
const def = protoLoader.loadSync(path.join(__dirname, '../../proto/payment.proto'), {
  keepCase: true, longs: String, defaults: true,
});
const { PaymentService } = grpc.loadPackageDefinition(def).payment;

const fs = require('fs');
const certs = path.join(__dirname, '../../certs');
// The mTLS identity: the CA to trust plus this client's own key and
// certificate. These files are secrets and are NOT in git (certs/ is ignored).
const credentials = grpc.credentials.createSsl(
  fs.readFileSync(path.join(certs, 'ca.crt')),
  fs.readFileSync(path.join(certs, 'order.key')),
  fs.readFileSync(path.join(certs, 'order.crt'))
);

const client = new PaymentService('localhost:50051', credentials);

// Make the call and print what comes back. An error has a gRPC status code
// (for example 14 = unavailable, 3 = invalid argument).
client.Charge({ order_id: 1, amount: 49.99 }, (err, res) => {
  if (err) return console.error('gRPC error:', err.code, err.message);
  console.log('Response:', res);
});

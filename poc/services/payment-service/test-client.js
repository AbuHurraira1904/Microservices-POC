const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const def = protoLoader.loadSync(path.join(__dirname, '../../proto/payment.proto'), {
  keepCase: true, longs: String, defaults: true,
});
const { PaymentService } = grpc.loadPackageDefinition(def).payment;

const fs = require('fs');
const certs = path.join(__dirname, '../../certs');
const credentials = grpc.credentials.createSsl(
  fs.readFileSync(path.join(certs, 'ca.crt')),
  fs.readFileSync(path.join(certs, 'order.key')),
  fs.readFileSync(path.join(certs, 'order.crt'))
);

// const credentials = grpc.credentials.createSsl(
//   fs.readFileSync(path.join(certs, 'ca.crt'))
// );

const client = new PaymentService('localhost:50051', credentials);

//const client = new PaymentService('localhost:50051', grpc.credentials.createInsecure());

client.Charge({ order_id: 1, amount: 49.99 }, (err, res) => {
  if (err) return console.error('gRPC error:', err.code, err.message);
  console.log('Response:', res);
});
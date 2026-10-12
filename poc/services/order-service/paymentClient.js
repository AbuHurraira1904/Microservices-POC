const path = require('path');
const fs = require('fs');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_PATH = path.join(__dirname, '../../proto/payment.proto');
const CERTS_DIR = process.env.CERTS_DIR || path.join(__dirname, '../../certs');
const readCert = (name) => fs.readFileSync(path.join(CERTS_DIR, name));

const def = protoLoader.loadSync(PROTO_PATH, { keepCase: true, longs: String, defaults: true });
const { PaymentService } = grpc.loadPackageDefinition(def).payment;

const credentials = grpc.credentials.createSsl(
  readCert('ca.crt'),
  readCert('order.key'),
  readCert('order.crt')
);

// const credentials = grpc.credentials.createSsl(
//   fs.readFileSync(path.join(certs, 'ca.crt'))
// );

//const client = new PaymentService(process.env.PAYMENT_SERVICE_ADDR,, grpc.credentials.createInsecure());


const client = new PaymentService(process.env.PAYMENT_SERVICE_ADDR, credentials);

// Promise wrapper with a deadline so a hung Payment service cannot stall the request
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
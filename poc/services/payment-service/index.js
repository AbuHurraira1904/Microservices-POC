    require('dotenv').config();
    const path = require('path');
    const crypto = require('crypto');
    const grpc = require('@grpc/grpc-js');
    const protoLoader = require('@grpc/proto-loader');

    const PROTO_PATH = path.join(__dirname, '../../proto/payment.proto');
    const PORT = process.env.PORT || 50051;
    const FAILURE_RATE = parseFloat(process.env.FAILURE_RATE ?? '0.2');
    const LATENCY_MS = parseInt(process.env.LATENCY_MS ?? '300', 10);

    const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,   // keep field names like order_id instead of orderId
    longs: String,
    defaults: true,
    });
    const paymentProto = grpc.loadPackageDefinition(packageDefinition).payment;

    const fs = require('fs');
    const CERTS_DIR = process.env.CERTS_DIR || path.join(__dirname, '../../certs');
    const readCert = (name) => fs.readFileSync(path.join(CERTS_DIR, name));

    function charge(call, callback) {
    const { order_id, amount } = call.request;

    // Basic validation: reject bad input with a proper gRPC status, not an "approved: false"
    if (!order_id || !(amount > 0)) {
        return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'order_id and a positive amount are required',
        });
    }

    // Simulate the time a real processor would take
    setTimeout(() => {
        const approved = Math.random() >= FAILURE_RATE;
        const transaction_id = crypto.randomUUID();

        console.log(
        `[charge] order=${order_id} amount=${amount} -> ${approved ? 'APPROVED' : 'DECLINED'} (${transaction_id})`
        );

        callback(null, {
        approved,
        transaction_id,
        reason: approved ? '' : 'Declined by simulated processor',
        });
    }, LATENCY_MS);
    }

    const server = new grpc.Server();
    server.addService(paymentProto.PaymentService.service, { Charge: charge });

    const credentials = grpc.ServerCredentials.createSsl(
    readCert('ca.crt'),                       // CA used to verify client certs
    [{ private_key: readCert('payment.key'),  // this server's identity
        cert_chain: readCert('payment.crt') }],
    true                                      // true = require a client cert (mTLS)
    );

    server.bindAsync(
    `0.0.0.0:${PORT}`,
    credentials,
    (err, port) => {
        if (err) {
        console.error('Failed to start Payment service:', err);
        process.exit(1);
        }
        console.log(`Payment service (gRPC) on port ${port}, failure rate ${FAILURE_RATE}`);
    }
    );
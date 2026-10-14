    // =========================================================================
    // PAYMENT SERVICE  (gRPC over mTLS, port 50051)
    // =========================================================================
    // A SIMULATED payment processor. It "charges" an order by waiting a moment
    // and then approving or declining at random. No real money moves and
    // nothing is stored. Its job in this demo is to be a realistic, slightly
    // unreliable dependency, so the Order service has something to fail
    // against (declines, slowness).
    //
    // Words used in this file
    //   - gRPC: a way for services to call each other as if calling a function.
    //     The exact shape of the call is written in a contract file,
    //     proto/payment.proto, which both sides load.
    //   - mTLS ("mutual TLS"): encrypted connection where BOTH sides prove who
    //     they are with a certificate. The server checks the client too, so
    //     only a service that holds a certificate signed by our own certificate
    //     authority (CA) can call Payment.
    //   - Certificate authority (CA): the trusted signer. Here a self-made one,
    //     whose public certificate is ca.crt.
    //
    // Settings (in .env, with these defaults)
    //   - PORT          50051
    //   - FAILURE_RATE  0.2  share of charges that are declined (0 to 1)
    //   - LATENCY_MS    300  pretend processing time per charge
    //   - CERTS_DIR     ../../certs  folder holding ca.crt, payment.key, payment.crt
    //
    // Decisions made here, and why
    //   - Why gRPC: Payment is an internal service called only by our own
    //     services, where a strict contract, compact messages and early error
    //     detection pay off. (A real card processor is reached over REST and
    //     webhooks instead.)
    //   - Why mTLS: money is involved. A caller must prove its identity, not
    //     just reach the port. This is also the demo of securing the channel
    //     between services.
    //   - A declined payment is a normal ANSWER (approved = false), not an
    //     error. Errors are reserved for broken requests and broken systems, so
    //     the caller can tell "the bank said no" apart from "something is wrong".
    //   - Bad input is rejected with the proper gRPC status code
    //     (INVALID_ARGUMENT), for the same reason.
    //   - Failure rate and delay are settings so the demo can show the failure
    //     paths on demand: 1.0 declines everything; a high LATENCY_MS makes the
    //     Order service's 3-second limit trigger (HTTP 504).
    //
    // PRODUCTION CHANGES (what a full system would do differently)
    //   - Talk to a real payment provider, through its own API, with its own
    //     authentication. This file would become a thin wrapper around it.
    //   - Idempotency: accept a unique key per charge and remember the result.
    //     The same key sent twice (a retry after a timeout) must return the
    //     first result and never charge again. Today every call is a new
    //     charge, which is why retries are unsafe.
    //   - Store every charge (a payments table with status, amount, order id,
    //     timestamps) and publish events such as "payment approved/declined".
    //   - Use whole cents (an integer) for amounts, not a decimal number, and
    //     include the currency.
    //   - Certificates would be issued by a managed CA, short-lived and
    //     rotated automatically, with the private keys kept in a secrets store.
    //   - Bind to the private network address only (today it listens on every
    //     network interface, 0.0.0.0), and add health checks, metrics, request
    //     logging with ids, and graceful shutdown.
    // =========================================================================

    // Load the settings from the .env file first.
    require('dotenv').config();
    const path = require('path');
    const crypto = require('crypto');
    const grpc = require('@grpc/grpc-js');
    const protoLoader = require('@grpc/proto-loader');

    const PROTO_PATH = path.join(__dirname, '../../proto/payment.proto');
    const PORT = process.env.PORT || 50051;
    // `??` keeps a value of 0 (a valid setting: never decline). An EMPTY value
    // is not nullish, so it becomes NaN and every charge would be declined;
    // that is why .env.example gives FAILURE_RATE a number.
    const FAILURE_RATE = parseFloat(process.env.FAILURE_RATE ?? '0.2');
    const LATENCY_MS = parseInt(process.env.LATENCY_MS ?? '300', 10);

    // Read the contract (payment.proto) and build the service definition from it.
    const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,   // keep field names like order_id instead of orderId
    longs: String,
    defaults: true,
    });
    const paymentProto = grpc.loadPackageDefinition(packageDefinition).payment;

    const fs = require('fs');
    const CERTS_DIR = process.env.CERTS_DIR || path.join(__dirname, '../../certs');
    const readCert = (name) => fs.readFileSync(path.join(CERTS_DIR, name));

    // The one operation this service offers: Charge.
    // `call.request` holds what the client sent; `callback(error, answer)` is
    // how we reply.
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
        // Math.random() is a number from 0 up to (not including) 1. The charge
        // is approved when it is at least FAILURE_RATE, so FAILURE_RATE 0.2
        // declines about 1 charge in 5.
        const approved = Math.random() >= FAILURE_RATE;
        // A unique reference for this charge, like a bank's receipt number.
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

    // Register the operation under the service name defined in the contract.
    const server = new grpc.Server();
    server.addService(paymentProto.PaymentService.service, { Charge: charge });

    // The mTLS setup. The final `true` is the important part: it makes the
    // server DEMAND a client certificate and check it against the CA. Without
    // it this would be ordinary one-way TLS and any caller could connect.
    const credentials = grpc.ServerCredentials.createSsl(
    readCert('ca.crt'),                       // CA used to verify client certs
    [{ private_key: readCert('payment.key'),  // this server's identity
        cert_chain: readCert('payment.crt') }],
    true                                      // true = require a client cert (mTLS)
    );

    // Start listening. If the port cannot be opened (for example it is already
    // in use) the service exits with an error instead of running without
    // listening.
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

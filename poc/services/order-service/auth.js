// =============================================================================
// AUTHENTICATION AND PERMISSIONS  (Order service)
// =============================================================================
// Answers two questions for every protected request:
//   1. WHO is calling?          (authentication)
//   2. MAY they do this?        (authorisation, decided by the caller's "role")
//
// How it works: the JWT
//   A JWT ("JSON Web Token") is a small signed note, for example
//   "user: admin, role: admin, valid until 15:00". The server signs it with a
//   secret key when someone logs in. Later requests carry the token, and any
//   service that knows the secret can check the signature WITHOUT asking a
//   login service or a database. A token that was changed, or signed with a
//   different secret, fails the check.
//
// The three roles in this system
//   - customer: may place orders and read stock levels.
//   - admin:    may also list all orders and create stock.
//   - service:  the identity of this Order service itself when it calls
//               Inventory's internal endpoints (reserve, commit, release).
//
// Decisions made here, and why
//   - The algorithm (HS256) is fixed on the server when verifying. A token's
//     own header says which algorithm it uses; an attacker could write
//     "alg: none" (no signature at all) and, in a careless server, be accepted.
//     Pinning the algorithm closes that whole class of attacks.
//   - The service refuses to start without a secret of 32+ characters. A
//     missing or short secret would otherwise leave the service running with
//     no real protection and nobody noticing.
//   - Two different failure codes: 401 means "I don't know who you are"
//     (no, invalid or expired token); 403 means "I know who you are, and you
//     are not allowed to do this".
//   - When Order calls Inventory it creates its OWN short-lived token with the
//     role "service" instead of forwarding the customer's token. If it
//     forwarded the customer's token, any customer could call Inventory's
//     internal endpoints (such as "release stock") directly with it.
//   - Tokens expire (1 hour for people, 5 minutes for the service), so a
//     leaked token stops working on its own.
//
// PRODUCTION CHANGES (what a full system would do differently)
//   - Use asymmetric keys (RS256 or ES256): a private key that only the login
//     service holds, and a public key that every other service uses to check
//     tokens. Today all services share ONE secret, so any of them could create
//     a token for any role.
//   - Move login into a separate identity service (or a product such as Auth0
//     or Keycloak), with hashed passwords, rate limiting and account lockout.
//   - Add a way to cancel a token before it expires (a revocation list, or
//     short-lived access tokens plus refresh tokens). Today a token stays
//     valid until it expires.
//   - Add the issuer and audience claims ("who made this token", "who is it
//     for") and check them.
//   - Rotate the secret/keys regularly, and store them in a secrets manager
//     rather than a .env file.
//   - Service-to-service identity is usually handled with mTLS or a service
//     mesh instead of tokens (this PoC already uses mTLS for Payment).
//
// Keep this module's require() after dotenv.config() in index.js, because it
// reads JWT_SECRET from process.env when it loads.
// =============================================================================

const jwt = require('jsonwebtoken');

const SECRET = process.env.JWT_SECRET;
// Fail at startup, not at the first request: better to stop loudly than to run
// unprotected.
if (!SECRET || SECRET.length < 32) {
  throw new Error('JWT_SECRET must be set (at least 32 characters). Generate one with: openssl rand -hex 32');
}

// HS256 = both signing and checking use the same shared secret ("symmetric").
const ALGORITHM = 'HS256';
const USER_TOKEN_TTL = '1h';
const SERVICE_TOKEN_TTL = '5m';

// Creates the token for a person who has just logged in. "sub" (subject) is
// who the token is about; "role" is what they may do.
function signUserToken(username, role) {
  return jwt.sign({ sub: username, role }, SECRET, { algorithm: ALGORITHM, expiresIn: USER_TOKEN_TTL });
}

// Short-lived token Order uses when it calls Inventory. Deliberately NOT the
// caller's token: if we forwarded it, any customer could call Inventory's
// reserve/commit/release directly with their own token.
function mintServiceToken() {
  return jwt.sign({ sub: 'order-service', role: 'service' }, SECRET, {
    algorithm: ALGORITHM,
    expiresIn: SERVICE_TOKEN_TTL,
  });
}

// Express "middleware": a gate placed in front of a route. Usage:
//   app.get('/orders', requireRole('admin'), handler)
// The handler only runs if the request carries a valid token whose role is in
// the allowed list. Otherwise the gate answers 401 or 403 and stops.
function requireRole(...roles) {
  return (req, res, next) => {
    // The token travels in the header:  Authorization: Bearer <token>
    const header = req.headers.authorization || '';
    const [scheme, token] = header.split(' ');
    if (scheme !== 'Bearer' || !token) {
      return res.status(401).json({ error: 'missing bearer token' });
    }

    let claims;
    try {
      // Checks the signature, the expiry time, and (via `algorithms`) that the
      // token uses the algorithm WE chose, not the one the token asks for.
      claims = jwt.verify(token, SECRET, { algorithms: [ALGORITHM] });
    } catch (err) {
      const message = err.name === 'TokenExpiredError' ? 'token expired' : 'invalid token';
      return res.status(401).json({ error: message });
    }

    // Authenticated, but is this role allowed on this route?
    if (!roles.includes(claims.role)) {
      return res.status(403).json({ error: 'forbidden for this role' });
    }

    // Make the caller's identity available to the route handler.
    req.user = { sub: claims.sub, role: claims.role };
    next();
  };
}

module.exports = { signUserToken, mintServiceToken, requireRole };

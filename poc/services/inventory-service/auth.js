// =============================================================================
// TOKEN CHECKING  (Inventory service)
// =============================================================================
// Inventory only CHECKS tokens; it never creates them (the Order service does,
// see order-service/auth.js, which explains JWTs, roles and the reasoning in
// more detail). Each service checks tokens on its own, so there is no single
// gateway that, if bypassed, would leave every service open.
//
// What it protects (the routes choose the roles in index.js)
//   - reserve / commit / release: only role "service" (the Order service).
//   - create item: only role "admin".
//   - read stock: any valid role.
//
// Decisions made here, and why
//   - The check is repeated here, not skipped because "Order already checked".
//     Anyone who can reach this service's port directly would otherwise bypass
//     Order completely. Each service protects itself ("never trust the
//     network").
//   - HS256 is fixed on the server when verifying, so a token cannot choose
//     its own algorithm (blocks the "alg: none" trick).
//   - The service refuses to start without a secret of 32+ characters, so it
//     can never run silently unprotected.
//   - 401 = we do not know who you are (no, invalid or expired token).
//     403 = we know who you are, and this role may not do this.
//
// PRODUCTION CHANGES (what a full system would do differently)
//   - Do not share one secret between services. Use asymmetric keys (RS256):
//     only the login service holds the private key; Inventory would hold just
//     the public key, which can only CHECK tokens, never create them. Today
//     anyone who can read this service's secret could create an admin token.
//   - Fetch the public keys from the identity provider (a "JWKS" address) so
//     keys can be rotated without redeploying.
//   - Check the issuer and audience claims, and support token revocation.
//   - Keep the secret in a secrets manager, not a .env file.
//
// Keep this module's require() after dotenv.config() in index.js, because it
// reads JWT_SECRET from process.env when it loads.
// =============================================================================

const jwt = require('jsonwebtoken');

const SECRET = process.env.JWT_SECRET;
// Stop at startup rather than run unprotected. The value must be the SAME as
// in the Order service, or Inventory would reject every token Order creates.
if (!SECRET || SECRET.length < 32) {
  throw new Error('JWT_SECRET must be set (at least 32 characters, same value as Order). Generate one with: openssl rand -hex 32');
}

const ALGORITHM = 'HS256';

// Express "middleware": a gate placed in front of a route, used like
//   app.post('/inventory', requireRole('admin'), handler)
// The handler runs only if the request carries a valid token whose role is in
// the allowed list.
function requireRole(...roles) {
  return (req, res, next) => {
    // The token is sent as:  Authorization: Bearer <token>
    const header = req.headers.authorization || '';
    const [scheme, token] = header.split(' ');
    if (scheme !== 'Bearer' || !token) {
      return res.status(401).json({ error: 'missing bearer token' });
    }

    let claims;
    try {
      // Verifies the signature and the expiry time, and only accepts the
      // algorithm we chose.
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

module.exports = { requireRole };

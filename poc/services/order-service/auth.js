// JWT helpers for the Order service.
//
// Security choices:
//  - HS256 is pinned in verify(): the token's own header cannot choose the
//    algorithm, which blocks "alg: none" and algorithm-confusion attacks.
//  - The service refuses to start without a long JWT_SECRET.
//  - 401 = no/invalid/expired token (we do not know who you are).
//    403 = valid token, wrong role (we know who you are; you may not do this).
//
// Known limits (PoC): one shared secret means any service holding it can mint
// any token. A real system uses asymmetric keys (RS256) so only the identity
// service can sign. Order also acts as a stub login service; a real system would
// use a separate identity provider.
//
// Keep this module's require() after dotenv.config() in index.js.

const jwt = require('jsonwebtoken');

const SECRET = process.env.JWT_SECRET;
if (!SECRET || SECRET.length < 32) {
  throw new Error('JWT_SECRET must be set (at least 32 characters). Generate one with: openssl rand -hex 32');
}

const ALGORITHM = 'HS256';
const USER_TOKEN_TTL = '1h';
const SERVICE_TOKEN_TTL = '5m';

// Token for a logged-in person.
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

// Express middleware: require a valid Bearer token whose role is one of `roles`.
function requireRole(...roles) {
  return (req, res, next) => {
    const header = req.headers.authorization || '';
    const [scheme, token] = header.split(' ');
    if (scheme !== 'Bearer' || !token) {
      return res.status(401).json({ error: 'missing bearer token' });
    }

    let claims;
    try {
      claims = jwt.verify(token, SECRET, { algorithms: [ALGORITHM] });
    } catch (err) {
      const message = err.name === 'TokenExpiredError' ? 'token expired' : 'invalid token';
      return res.status(401).json({ error: message });
    }

    if (!roles.includes(claims.role)) {
      return res.status(403).json({ error: 'forbidden for this role' });
    }

    req.user = { sub: claims.sub, role: claims.role };
    next();
  };
}

module.exports = { signUserToken, mintServiceToken, requireRole };

// JWT verification for the Inventory service. (Inventory only checks tokens; it
// never issues them. Order issues them.)
//
// Security choices:
//  - HS256 is pinned in verify(): the token's own header cannot choose the
//    algorithm, which blocks "alg: none" and algorithm-confusion attacks.
//  - The service refuses to start without a long JWT_SECRET.
//  - 401 = no/invalid/expired token (we do not know who you are).
//    403 = valid token, wrong role (we know who you are; you may not do this).
//
// Known limit (PoC): the secret is shared with Order, so any service holding it
// can mint any token. A real system uses asymmetric keys (RS256).
//
// Keep this module's require() after dotenv.config() in index.js.

const jwt = require('jsonwebtoken');

const SECRET = process.env.JWT_SECRET;
if (!SECRET || SECRET.length < 32) {
  throw new Error('JWT_SECRET must be set (at least 32 characters, same value as Order). Generate one with: openssl rand -hex 32');
}

const ALGORITHM = 'HS256';

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

module.exports = { requireRole };

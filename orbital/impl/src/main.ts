import { Elysia, t } from 'elysia';

const ORBITAL_PORT = process.env.ORBITAL_PORT || 3002;

const app = new Elysia()
  .post('/v1/advisory/score-placement', ({ body }) => {
    // Static response — real scoring comes in ADVISORY-02
    return {
      recommendations: [
        { provider: 'orbstack', region: 'local', score: 100,
          reason: 'local development' },
      ],
      data_staleness_ms: 0,
    };
  }, {
    body: t.Object({
      spec: t.String(),
      region_preferences: t.Optional(t.Array(t.String())),
      data_locality_hints: t.Optional(t.Array(t.String())),
    }),
  })
  .get('/v1/advisory/health', () => ({ status: 'ok' }))
  .listen({ port: Number(ORBITAL_PORT), idleTimeout: 0 });

console.log(`Orbital advisory API listening on :${ORBITAL_PORT}`);

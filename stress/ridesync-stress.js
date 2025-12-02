#!/usr/bin/env node

const { setTimeout: delay } = require("node:timers/promises");
const { performance } = require("node:perf_hooks");
const crypto = require("node:crypto");

main().catch((err) => {
  console.error("[RideSync Stress] Fatal error", err);
  process.exit(1);
});

async function main() {
  const args = parseArgs(process.argv);
  const config = buildConfig(args);

  if (!config.tokenPool.length) {
    console.error(
      "[RideSync Stress] Provide at least one Firebase ID token via --idToken or STRESS_ID_TOKEN."
    );
    process.exit(1);
  }

  const scenarioPool = buildScenarioPool(config);
  if (!scenarioPool.length) {
    console.error(
      "[RideSync Stress] No valid scenarios configured. Check --scenarios flag."
    );
    process.exit(1);
  }

  const metrics = new Map();
  const startTs = Date.now();
  const deadline = startTs + config.durationMs;
  let shouldStop = false;

  process.on("SIGINT", () => {
    if (shouldStop) {
      return;
    }
    shouldStop = true;
    console.warn("\n[RideSync Stress] Caught SIGINT, draining workers...");
  });

  console.log(
    `\n[RideSync Stress] Target: ${config.callableBase}` +
      `\n  Workers : ${config.concurrency}` +
      `\n  Duration: ${config.durationMs} ms` +
      `\n  Think   : ${config.thinkTimeMs} ms` +
      `\n  Scenarios: ${config.scenarioSpec}` +
      (config.enableStripe ? "\n  Stripe : enabled" : "\n  Stripe : skipped") +
      `\n`
  );

  const workers = [];
  for (let i = 0; i < config.concurrency; i += 1) {
    const token = config.tokenPool[i % config.tokenPool.length];
    const workerState = {
      workerId: i,
      token,
      uid: decodeUidFromToken(token) || `loadtest-${i}`,
      profileReady: false,
    };
    workers.push(
      runWorker({
        config,
        workerState,
        scenarioPool,
        metrics,
        deadline,
        shouldStopFn: () => shouldStop,
      })
    );
  }

  await Promise.all(workers);
  const totalMs = Date.now() - startTs;
  printSummary({ metrics, totalMs, config });

  const hadFailures = Array.from(metrics.values()).some((entry) => entry.fail > 0);
  process.exit(hadFailures ? 2 : 0);
}

async function runWorker({
  config,
  workerState,
  scenarioPool,
  metrics,
  deadline,
  shouldStopFn,
}) {
  while (!shouldStopFn() && Date.now() < deadline) {
    const scenario = pickScenario(scenarioPool);
    const t0 = performance.now();
    try {
      await scenario.fn(workerState, config);
      const elapsed = performance.now() - t0;
      recordMetric(metrics, scenario.name, true, elapsed);
    } catch (err) {
      const elapsed = performance.now() - t0;
      recordMetric(metrics, scenario.name, false, elapsed, err);
    }
    if (config.thinkTimeMs > 0) {
      await delay(config.thinkTimeMs);
    }
  }
}

function recordMetric(store, name, ok, durationMs, err) {
  const entry = store.get(name) || {
    name,
    count: 0,
    ok: 0,
    fail: 0,
    sumMs: 0,
    maxMs: 0,
    samples: [],
  };
  entry.count += 1;
  if (ok) {
    entry.ok += 1;
    entry.sumMs += durationMs;
    if (durationMs > entry.maxMs) {
      entry.maxMs = durationMs;
    }
    if (entry.samples.length < 5) {
      entry.samples.push(durationMs.toFixed(2));
    }
  } else {
    entry.fail += 1;
    if (entry.samples.length < 5) {
      entry.samples.push(`ERR:${err?.message || err}`);
    }
  }
  store.set(name, entry);
}

function printSummary({ metrics, totalMs, config }) {
  console.log("\n[RideSync Stress] Completed in", totalMs.toFixed(0), "ms");
  const rows = Array.from(metrics.values()).map((entry) => {
    const avgMs = entry.ok ? entry.sumMs / entry.ok : 0;
    return {
      scenario: entry.name,
      count: entry.count,
      ok: entry.ok,
      fail: entry.fail,
      avgMs: Number(avgMs.toFixed(2)),
      maxMs: Number(entry.maxMs.toFixed(2)),
      samples: entry.samples.join(", "),
    };
  });
  rows.sort((a, b) => b.count - a.count);
  rows.forEach((row) => {
    console.log(
      `${row.scenario.padEnd(18)} ` +
        `total=${row.count.toString().padStart(5)} ` +
        `ok=${row.ok.toString().padStart(5)} ` +
        `fail=${row.fail.toString().padStart(5)} ` +
        `avg=${row.avgMs.toFixed(2).padStart(8)}ms ` +
        `max=${row.maxMs.toFixed(2).padStart(8)}ms ` +
        `samples=[${row.samples}]`
    );
  });
  console.log("\n[RideSync Stress] Scenario registry:");
  Object.entries(scenarioRegistry)
    .filter(([, meta]) => !meta.requiresStripe || config.enableStripe)
    .forEach(([key, meta]) => {
      console.log(` - ${key}: ${meta.description}`);
    });
}

function pickScenario(pool) {
  const idx = Math.floor(Math.random() * pool.length);
  return pool[idx];
}

function buildScenarioPool(config) {
  const parsed = parseScenarioSpec(config.scenarioSpec);
  const pool = [];
  parsed.forEach(({ name, weight }) => {
    const meta = scenarioRegistry[name];
    if (!meta) {
      console.warn(`[RideSync Stress] Unknown scenario '${name}', skipping.`);
      return;
    }
    if (meta.requiresStripe && !config.enableStripe) {
      console.warn(`[RideSync Stress] Scenario '${name}' requires Stripe, skipped.`);
      return;
    }
    for (let i = 0; i < weight; i += 1) {
      pool.push({ name, fn: meta.fn });
    }
  });
  return pool;
}

function parseScenarioSpec(spec) {
  return spec
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean)
    .map((entry) => {
      const [name, weightStr] = entry.split(":");
      const weight = Math.max(1, Number(weightStr) || 1);
      return { name, weight };
    });
}

const scenarioRegistry = {
  profile: {
    description: "saveUserProfile",
    fn: runProfileScenario,
  },
  ride: {
    description: "createRideRequest (+optional cancel)",
    fn: runRideScenario,
  },
  checkout: {
    description: "createRideCheckoutSessionCallable",
    fn: runRideCheckoutScenario,
    requiresStripe: true,
  },
  membership: {
    description: "createMembershipCheckoutSession",
    fn: runMembershipScenario,
    requiresStripe: true,
  },
  stats: {
    description: "getDriverAvailabilityStats",
    fn: runDriverStatsScenario,
  },
};

async function runProfileScenario(state, config) {
  const payload = buildProfilePayload(state);
  await callCallable({
    config,
    functionName: "saveUserProfile",
    data: payload,
    token: state.token,
  });
  state.profileReady = true;
}

async function runRideScenario(state, config) {
  if (!state.profileReady) {
    await runProfileScenario(state, config);
  }
  const ride = buildRidePayload();
  const response = await callCallable({
    config,
    functionName: "createRideRequest",
    data: { ride },
    token: state.token,
  });
  const rideId = response?.rideId;
  if (rideId && Math.random() < config.cancelRatio) {
    await callCallable({
      config,
      functionName: "cancelRideRequest",
      data: { rideId },
      token: state.token,
    }).catch(() => {});
  }
}

async function runRideCheckoutScenario(state, config) {
  if (!state.profileReady) {
    await runProfileScenario(state, config);
  }
  const ride = buildRidePayload();
  const amountCents = Math.max(ride.totalCents || 1200, 600);
  await callCallable({
    config,
    functionName: "createRideCheckoutSessionCallable",
    data: {
      amountCents,
      ride,
      description: ride.description,
      redirectBaseUrl: config.redirectBaseUrl,
    },
    token: state.token,
  });
}

async function runMembershipScenario(state, config) {
  if (!state.profileReady) {
    await runProfileScenario(state, config);
  }
  await callCallable({
    config,
    functionName: "createMembershipCheckoutSession",
    data: {
      plan: Math.random() < 0.5 ? "uofa_unlimited" : "nwa_unlimited",
      mode: "subscription",
      redirectBaseUrl: config.redirectBaseUrl,
    },
    token: state.token,
  });
}

async function runDriverStatsScenario(state, config) {
  await callCallable({
    config,
    functionName: "getDriverAvailabilityStats",
    data: {},
    token: state.token,
  });
}

async function callCallable({ config, functionName, data, token }) {
  const url = `${config.callableBase}/${functionName}`;
  const body = JSON.stringify({ data });
  return invokeHttp({
    url,
    body,
    token,
  });
}

async function invokeHttp({ url, body, token, method = "POST" }) {
  const headers = {
    "Content-Type": "application/json",
  };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  const response = await fetch(url, { method, headers, body });
  const payload = await readJson(response);
  if (payload?.error) {
    throw new Error(payload.error.message || JSON.stringify(payload.error));
  }
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  if (payload && Object.prototype.hasOwnProperty.call(payload, "result")) {
    return payload.result;
  }
  return payload;
}

async function readJson(res) {
  const text = await res.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch (err) {
    throw new Error(`Invalid JSON response: ${text.slice(0, 200)}`);
  }
}

function buildProfilePayload(state) {
  const genders = ["female", "male"];
  const streetNames = [
    "W Center St",
    "S School Ave",
    "N Leverett Ave",
    "Maple St",
    "Dickson St",
  ];
  const firstNames = ["Alex", "Jordan", "Casey", "Taylor", "Riley"];
  const lastNames = ["Quinn", "Hayes", "Morgan", "Baker", "Cooper"];
  const fullName = `${pick(firstNames)} ${pick(lastNames)}-${state.workerId}`;
  return {
    fullName,
    gender: pick(genders),
    phone: `479${Math.floor(1000000 + Math.random() * 8999999)}`,
    street: `${100 + Math.floor(Math.random() * 900)} ${pick(streetNames)}`,
    city: "Fayetteville",
    state: "AR",
    zip: "72701",
    isStudent: Math.random() < 0.5,
    membershipTermsAccepted: true,
    membershipTermsVersion: "v1",
  };
}

function buildRidePayload() {
  const pickup = randomCoordinate();
  const dropoff = randomCoordinate();
  const estimatedDurationMinutes = 8 + Math.floor(Math.random() * 23);
  const totalCents = Math.max(500, estimatedDurationMinutes * 120);
  const extraStops = [];
  if (Math.random() < 0.25) {
    extraStops.push({
      label: "Extra Stop",
      location: randomCoordinate(),
    });
  }
  return {
    pickupLocation: pickup,
    dropoffLocation: dropoff,
    pickupAddress: `${Math.floor(
      100 + Math.random() * 900
    )} N College Ave, Fayetteville, AR`,
    dropoffAddress: `${Math.floor(100 + Math.random() * 900)} W Maple St, Fayetteville, AR`,
    pickupCity: "Fayetteville",
    dropoffCity: "Fayetteville",
    pickupState: "AR",
    dropoffState: "AR",
    numRiders: Math.random() < 0.2 ? 2 : 1,
    maxRiders: 2,
    isGroupRide: Math.random() < 0.3,
    estimatedDurationMinutes,
    totalCents,
    distanceKm: Number((1 + Math.random() * 8).toFixed(2)),
    description: `Load test ride ${crypto.randomUUID().slice(0, 8)}`,
    extraStops,
    notes: "load-test",
  };
}

function randomCoordinate() {
  const baseLat = 36.072;
  const baseLng = -94.16;
  const lat = baseLat + (Math.random() - 0.5) * 0.08;
  const lng = baseLng + (Math.random() - 0.5) * 0.08;
  return { lat: Number(lat.toFixed(6)), lng: Number(lng.toFixed(6)) };
}

function pick(list) {
  return list[Math.floor(Math.random() * list.length)];
}

function buildConfig(args) {
  const baseUrl = args.baseUrl || process.env.STRESS_BASE_URL || "http://127.0.0.1:5001";
  const projectId = args.project || process.env.STRESS_PROJECT_ID || "ridesync-emulator";
  const region = args.region || process.env.STRESS_REGION || "us-central1";
  const callableBase = `${baseUrl.replace(/\/$/, "")}/${projectId}/${region}`;
  const idTokensInput =
    args.idTokens || process.env.STRESS_ID_TOKENS || args.idToken || process.env.STRESS_ID_TOKEN || "";
  const tokenPool = idTokensInput
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean);
  const durationMs = Number(args.duration || process.env.STRESS_DURATION_MS || 60000);
  const thinkTimeMs = Number(args.think || process.env.STRESS_THINK_MS || 25);
  const cancelRatio = Number(args.cancelRatio || process.env.STRESS_CANCEL_RATIO || 0.25);
  const enableStripe = parseBoolean(
    args.enableStripe !== undefined ? args.enableStripe : process.env.STRESS_ENABLE_STRIPE || "false"
  );
  const redirectBaseUrl =
    args.redirectBaseUrl || process.env.STRESS_REDIRECT_BASE_URL || "https://ride-sync-nwa.web.app";
  return {
    baseUrl,
    projectId,
    region,
    callableBase,
    tokenPool,
    durationMs,
    thinkTimeMs,
    concurrency: Number(args.concurrency || process.env.STRESS_CONCURRENCY || 10),
    scenarioSpec: args.scenarios || process.env.STRESS_SCENARIOS || "profile:1,ride:4,stats:1",
    cancelRatio: Math.min(Math.max(cancelRatio, 0), 1),
    enableStripe,
    redirectBaseUrl,
  };
}

function parseArgs(argv) {
  return argv.slice(2).reduce((acc, item) => {
    if (!item.startsWith("--")) {
      return acc;
    }
    const eq = item.indexOf("=");
    if (eq === -1) {
      acc[item.slice(2)] = "true";
    } else {
      const key = item.slice(2, eq);
      acc[key] = item.slice(eq + 1);
    }
    return acc;
  }, {});
}

function parseBoolean(value) {
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = String(value || "").trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
}

function decodeUidFromToken(token) {
  if (!token || !token.includes(".")) {
    return null;
  }
  try {
    const base64 = token.split(".")[1];
    const normalized = base64.replace(/-/g, "+").replace(/_/g, "/");
    const payload = JSON.parse(Buffer.from(normalized, "base64").toString("utf8"));
    return payload.user_id || payload.sub || null;
  } catch (err) {
    return null;
  }
}

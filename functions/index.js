// functions/index.js

const functions = require("firebase-functions/v1");
const admin = require("firebase-admin");
const Stripe = require("stripe");
const { defineSecret } = require("firebase-functions/params");

// === RideSync Stripe debug helper ===
function logStripeDebug(label, payload = {}) {
  try {
    functions.logger.info(`[RideSync][Stripe] ${label}`, payload);
  } catch (e) {
    // Never let logging break the function
  }
}

const STRIPE_SECRET_ENV_NAMES = Object.freeze({
  secretKey: "STRIPE_SECRET_KEY",
  uofaPriceId: "STRIPE_UOFA_PRICE_ID",
  nwaPriceId: "STRIPE_NWA_PRICE_ID",
});

const STRIPE_CONFIG_DEBUG_FLAG = (
  process.env.STRIPE_CONFIG_DEBUG || ""
).toString().toLowerCase();
const STRIPE_CONFIG_DEBUG_ENABLED = ["1", "true", "debug", "verbose"].includes(
  STRIPE_CONFIG_DEBUG_FLAG
);

const STRIPE_SECRET_KEY = defineSecret("STRIPE_SECRET_KEY");
const STRIPE_UOFA_PRICE_ID = defineSecret("STRIPE_UOFA_PRICE_ID");
const STRIPE_NWA_PRICE_ID = defineSecret("STRIPE_NWA_PRICE_ID");
const STRIPE_SECRET_PARAMS = [
  STRIPE_SECRET_KEY,
  STRIPE_UOFA_PRICE_ID,
  STRIPE_NWA_PRICE_ID,
];

const runtimeConfig = (() => {
  try {
    return functions.config() || {};
  } catch (err) {
    return {};
  }
})();

// Never log secrets in cold start snapshots—this block intentionally removed.

function readEnvValue(envKey) {
  if (!envKey) {
    return null;
  }
  const env = process.env || {};
  return env[envKey] || null;
}

function pickRuntimeStripeValue(stripeConfig, ...keys) {
  if (!stripeConfig) {
    return null;
  }
  for (const key of keys) {
    if (key in stripeConfig && stripeConfig[key]) {
      return stripeConfig[key];
    }
  }
  return null;
}

function resolveStripeSettings() {
  const stripeConfig = runtimeConfig?.stripe || {};
  const configSecretKey = pickRuntimeStripeValue(stripeConfig, "secret_key", "secretKey");
  const configUofaPriceId = pickRuntimeStripeValue(
    stripeConfig,
    "uofa_price_id",
    "uofaPriceId"
  );
  const configNwaPriceId = pickRuntimeStripeValue(
    stripeConfig,
    "nwa_price_id",
    "nwaPriceId"
  );
  const envSecretKey = readEnvValue(STRIPE_SECRET_ENV_NAMES.secretKey);
  const envUofaPriceId = readEnvValue(STRIPE_SECRET_ENV_NAMES.uofaPriceId);
  const envNwaPriceId = readEnvValue(STRIPE_SECRET_ENV_NAMES.nwaPriceId);
  return {
    secretKey: configSecretKey || envSecretKey || null,
    uofaPriceId: configUofaPriceId || envUofaPriceId || null,
    nwaPriceId: configNwaPriceId || envNwaPriceId || null,
    sources: {
      secretKey: {
        config: configSecretKey || null,
        env: envSecretKey || null,
      },
      uofaPriceId: {
        config: configUofaPriceId || null,
        env: envUofaPriceId || null,
      },
      nwaPriceId: {
        config: configNwaPriceId || null,
        env: envNwaPriceId || null,
      },
    },
  };
}

function buildStripeConfigSnapshot() {
  const env = process.env || {};
  return {
    projectId: env.GCLOUD_PROJECT || env.GCP_PROJECT || null,
    secretKeyPresent: !!stripeSecretKey,
    uofaPriceIdPresent: !!uofaPriceId,
    nwaPriceIdPresent: !!nwaPriceId,
    rawEnvKeys: Object.keys(env).filter((key) =>
      key.toUpperCase().startsWith("STRIPE_")
    ),
  };
}

function logStripeConfigState(context, options = {}) {
  const {
    severity = "debug",
    includeEnvKeys = false,
    extra = null,
  } = options;
  const snapshot = buildStripeConfigSnapshot();
  const shouldIncludeEnvKeys = includeEnvKeys || severity === "ok";
  if (!shouldIncludeEnvKeys) {
    delete snapshot.rawEnvKeys;
  }
  const payload = {
    context,
    ...snapshot,
  };
  if (extra && typeof extra === "object") {
    payload.extra = extra;
  }
  if (severity === "ok") {
    console.log("[StripeConfigOK]", payload);
    return;
  }
  if (STRIPE_CONFIG_DEBUG_ENABLED || severity === "error") {
    console.log("[StripeConfigDebug]", payload);
  }
}

// --- Stripe config summary (Nov 30 2025) ---
// * All functions now read STRIPE_SECRET_KEY / STRIPE_UOFA_PRICE_ID / STRIPE_NWA_PRICE_ID
//   from Secret Manager first, with runtime config keys as a fallback.
// * logStripeConfigState surfaces `[StripeConfigOK]` (or `[StripeConfigDebug]` on errors)
//   so Cloud Functions logs show GCLOUD_PROJECT plus which values are present.
// * After pulling these changes run `firebase deploy --only functions` to ship them.

const stripeSettings = resolveStripeSettings();
const stripeSettingSources = stripeSettings.sources || {};
let stripe = null;
let stripeSecretKey = stripeSettings.secretKey || null;
let uofaPriceId = stripeSettings.uofaPriceId || null;
let nwaPriceId = stripeSettings.nwaPriceId || null;
if (stripeSettings.secretKey) {
  stripe = Stripe(stripeSettings.secretKey);
  stripeSecretKey = stripeSettings.secretKey;
} else {
  const configPresent = !!stripeSettingSources.secretKey?.config;
  const envPresent = !!stripeSettingSources.secretKey?.env;
  if (!configPresent && !envPresent) {
    console.warn(
      "[RideSync][Stripe] Missing Stripe secret key. Set stripe.secret_key runtime config or STRIPE_SECRET_KEY env to enable billing."
    );
    logStripeConfigState("init_missing_secret_key", {
      severity: "error",
      includeEnvKeys: true,
    });
  }
}
if (!uofaPriceId) {
  const configPresent = !!stripeSettingSources.uofaPriceId?.config;
  const envPresent = !!stripeSettingSources.uofaPriceId?.env;
  if (!configPresent && !envPresent) {
    console.warn(
      "[RideSync][Stripe] Missing U of A Stripe price ID. Set stripe.uofa_price_id or STRIPE_UOFA_PRICE_ID."
    );
    logStripeConfigState("init_missing_uofa_price", {
      severity: "error",
      includeEnvKeys: true,
      extra: { plan: "uofa_unlimited" },
    });
  }
}
if (!nwaPriceId) {
  const configPresent = !!stripeSettingSources.nwaPriceId?.config;
  const envPresent = !!stripeSettingSources.nwaPriceId?.env;
  if (!configPresent && !envPresent) {
    console.warn(
      "[RideSync][Stripe] Missing NWA Stripe price ID. Set stripe.nwa_price_id or STRIPE_NWA_PRICE_ID."
    );
    logStripeConfigState("init_missing_nwa_price", {
      severity: "error",
      includeEnvKeys: true,
      extra: { plan: "nwa_unlimited" },
    });
  }
}
// === RIDE SYNC STRIPE: END config ===

admin.initializeApp();

const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;
const Timestamp = admin.firestore.Timestamp;
const PIN_CHARSET = "0123456789";

const MAX_POOL_DISTANCE_KM = 3;
const CANDIDATE_LOOKBACK_MINUTES = 10;
const UOFA_PENDING_STATUSES = [
  "pending_driver",
  "pool_searching",
  "pooled_pending_driver",
];

const MEMBERSHIP_PLAN_DEFAULTS = {
  basic: {
    amountCents: 0,
    currency: "usd",
    label: "Basic (pay per ride)",
    durationDays: 0,
  },
  uofa_unlimited: {
    amountCents: 8000,
    currency: "usd",
    label: "U of A Unlimited ($80/mo)",
    durationDays: 30,
  },
  nwa_unlimited: {
    amountCents: 12000,
    currency: "usd",
    label: "NWA Unlimited ($120/mo)",
    durationDays: 30,
  },
};

const MEMBERSHIP_DEFAULT_DURATION_DAYS = 30;

const MEMBERSHIP_PLAN_ALIASES = {
  basic: new Set(["basic", "basic (pay per ride)", "basic plan", "plan: basic"]),
};

const COOLDOWN_ELIGIBLE_PLANS = new Set(["uofa_unlimited", "nwa_unlimited"]);
const MAX_EXTRA_STOPS = 3;
const RIDER_CANCELABLE_STATUSES = new Set([
  "pending_driver",
  "pooled_pending_driver",
  "pool_searching",
  "pending",
]);
const RIDER_RATING_MIN = 1;
const RIDER_RATING_MAX = 5;

const UNLIMITED_COOLDOWN_MINUTES =
  Number(runtimeConfig?.ridepolicy?.unlimited_cooldown_minutes) ||
  Number(process.env.RIDE_UNLIMITED_COOLDOWN_MINUTES) ||
  30;
const SURGE_MODE =
  runtimeConfig?.ridepolicy?.surge_mode === true ||
  process.env.RIDE_SURGE_MODE === "true";
const SURGE_COOLDOWN_MINUTES =
  Number(runtimeConfig?.ridepolicy?.surge_cooldown_minutes) ||
  Number(process.env.RIDE_SURGE_COOLDOWN_MINUTES) ||
  60;

const DEFAULT_FARE_CONSTANTS = {
  BASIC_RATE_PER_MIN: 0.6,
  BASIC_PLATFORM_FEE: 1.5,
  BASIC_PROCESSING_FEE_RATE: 0.03,
  UNLIMITED_OUT_RATE: 0.35,
  UNLIMITED_PROCESSING_FEE_RATE: 0.04,
};

function coercePositiveNumber(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string" && value.trim() === "") {
    return null;
  }
  const num = Number(value);
  return Number.isFinite(num) && num >= 0 ? num : null;
}

function resolveFareConstants() {
  const faresConfig = runtimeConfig?.fares || {};
  const env = process.env || {};
  const read = (envKey, configKey, fallback) => {
    const envValue = coercePositiveNumber(env[envKey]);
    if (envValue !== null) {
      return envValue;
    }
    const configValue = coercePositiveNumber(faresConfig[configKey]);
    if (configValue !== null) {
      return configValue;
    }
    return fallback;
  };

  return {
    BASIC_RATE_PER_MIN: read(
      "FARE_BASIC_RATE_PER_MIN",
      "basic_rate_per_min",
      DEFAULT_FARE_CONSTANTS.BASIC_RATE_PER_MIN
    ),
    BASIC_PLATFORM_FEE: read(
      "FARE_BASIC_PLATFORM_FEE",
      "basic_platform_fee",
      DEFAULT_FARE_CONSTANTS.BASIC_PLATFORM_FEE
    ),
    BASIC_PROCESSING_FEE_RATE: read(
      "FARE_BASIC_PROCESSING_FEE_RATE",
      "basic_processing_fee_rate",
      DEFAULT_FARE_CONSTANTS.BASIC_PROCESSING_FEE_RATE
    ),
    UNLIMITED_OUT_RATE: read(
      "FARE_UNLIMITED_OUT_RATE",
      "unlimited_out_rate",
      DEFAULT_FARE_CONSTANTS.UNLIMITED_OUT_RATE
    ),
    UNLIMITED_PROCESSING_FEE_RATE: read(
      "FARE_UNLIMITED_PROCESSING_FEE_RATE",
      "unlimited_processing_fee_rate",
      DEFAULT_FARE_CONSTANTS.UNLIMITED_PROCESSING_FEE_RATE
    ),
  };
}

const FARE_CONSTANTS = resolveFareConstants();

const RIDE_STATUS_ALLOWLIST = new Set([
  "pending_driver",
  "pool_searching",
  "pooled_pending_driver",
  "pending",
]);

const DRIVER_BUSY_STATUSES = [
  "driver_assigned",
  "arrived_at_pickup",
  "pickup_code_verified",
  "arrived_at_dropoff",
];

const POOL_GENDER_ALLOWLIST = new Set(["male", "female"]);

function normalizePoolGender(value) {
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  return POOL_GENDER_ALLOWLIST.has(normalized) ? normalized : null;
}

function gendersCompatible(genderA, genderB) {
  const normalizedA = normalizePoolGender(genderA);
  const normalizedB = normalizePoolGender(genderB);
  if (!normalizedA || !normalizedB) {
    return false;
  }
  return normalizedA === normalizedB;
}

function normalizeMembershipPlan(value = "") {
  const normalized = String(value ?? "").toLowerCase().trim();
  if (!normalized) {
    return "basic";
  }
  for (const [planKey, aliases] of Object.entries(MEMBERSHIP_PLAN_ALIASES)) {
    if (aliases.has(normalized) || normalized === planKey) {
      return planKey;
    }
  }
  return normalized;
}

function getSecretValue(secretParam) {
  if (!secretParam || typeof secretParam.value !== "function") {
    return null;
  }
  try {
    return secretParam.value() || null;
  } catch (err) {
    return null;
  }
}

function hydrateStripeSettingsFromSecrets() {
  const secretKey = getSecretValue(STRIPE_SECRET_KEY);
  const secretUofaPriceId = getSecretValue(STRIPE_UOFA_PRICE_ID);
  const secretNwaPriceId = getSecretValue(STRIPE_NWA_PRICE_ID);

  if (secretKey && secretKey !== stripeSecretKey) {
    stripe = Stripe(secretKey);
    stripeSecretKey = secretKey;
    // Log only the mode and tail of the key so we don't leak secrets
    logStripeDebug("hydrateStripeSettingsFromSecrets: stripe client updated", {
      keyStartsWithSkLive: secretKey.startsWith("sk_live_"),
      keyStartsWithSkTest: secretKey.startsWith("sk_test_"),
      keyTail: secretKey.slice(-6),
    });
  }
  if (secretUofaPriceId && secretUofaPriceId !== uofaPriceId) {
    uofaPriceId = secretUofaPriceId;
  }
  if (secretNwaPriceId && secretNwaPriceId !== nwaPriceId) {
    nwaPriceId = secretNwaPriceId;
  }
}

function getStripeClient(contextLabel = "getStripeClient") {
  hydrateStripeSettingsFromSecrets();
  if (!stripe) {
    logStripeConfigState(contextLabel, {
      severity: "error",
      includeEnvKeys: true,
    });
    throw new functions.https.HttpsError(
      "unavailable",
      "Stripe configuration is unavailable. Please contact RideSync support."
    );
  }
  logStripeConfigState(contextLabel, { severity: "ok" });
  return stripe;
}

function resolveMembershipPlan(planRaw = "") {
  const plan = normalizeMembershipPlan(planRaw);
  const defaults = MEMBERSHIP_PLAN_DEFAULTS[plan];
  if (!defaults) {
    return null;
  }

  const envAmount =
    process.env[`STRIPE_${plan.toUpperCase()}_CENTS`] ||
    runtimeConfig?.stripe?.[`${plan}_cents`];

  const amountCents = Number.isFinite(Number(envAmount))
    ? Number(envAmount)
    : defaults.amountCents;

  const durationDays =
    Number.isFinite(Number(defaults.durationDays)) && defaults.durationDays > 0
      ? Number(defaults.durationDays)
      : MEMBERSHIP_DEFAULT_DURATION_DAYS;

  return {
    ...defaults,
    amountCents,
    plan,
    durationDays,
  };
}

function computeMembershipExpirationTimestamp(durationDays = MEMBERSHIP_DEFAULT_DURATION_DAYS) {
  const days = Number(durationDays);
  if (!Number.isFinite(days) || days <= 0) {
    return null;
  }
  const durationMs = days * 24 * 60 * 60 * 1000;
  return Timestamp.fromMillis(Date.now() + durationMs);
}

async function maybeDowngradeExpiredMembership(userRef, profileData = {}) {
  if (!userRef) {
    return profileData || {};
  }
  const profile = profileData || {};
  const normalizedPlan = normalizeMembershipPlan(
    profile.membershipType || profile.membership || "basic"
  );
  if (normalizedPlan === "basic") {
    return profile;
  }
  const expiresAt = profile.membershipExpiresAt;
  if (!expiresAt || typeof expiresAt.toMillis !== "function") {
    return profile;
  }
  if (expiresAt.toMillis() > Date.now()) {
    return profile;
  }

  await userRef.set(
    {
      membershipType: "basic",
      membershipStatus: "expired",
      membershipExpiresAt: FieldValue.delete(),
      membershipExpiredAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  functions.logger.info("[RideSync][membership] auto-downgraded expired plan", {
    uid: userRef.id,
    previousPlan: normalizedPlan,
  });

  const refreshedSnap = await userRef.get();
  return refreshedSnap.exists ? refreshedSnap.data() || {} : {};
}

function computeFareForMembership(planRaw, minutesRaw, inHomeZone) {
  const plan = normalizeMembershipPlan(planRaw || "basic");
  const minutes = Number(minutesRaw);
  if (!Number.isFinite(minutes) || minutes <= 0) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "Ride duration is required for fare calculation."
    );
  }
  const clampedMinutes = Math.min(Math.max(minutes, 1), 600);
  const inZone = !!inHomeZone;

  const {
    BASIC_RATE_PER_MIN,
    BASIC_PLATFORM_FEE,
    BASIC_PROCESSING_FEE_RATE,
    UNLIMITED_OUT_RATE,
    UNLIMITED_PROCESSING_FEE_RATE,
  } = FARE_CONSTANTS;

  let rideSubtotal = 0;
  let processingFee = 0;
  let total = 0;
  let membershipLabel = "";

  if (plan === "uofa_unlimited" || plan === "nwa_unlimited") {
    if (inZone) {
      membershipLabel =
        plan === "uofa_unlimited"
          ? "U of A Unlimited – in Fayetteville (included)"
          : "NWA Unlimited – in zone (included)";
      return {
        rideSubtotal: 0,
        processingFee: 0,
        total: 0,
        membershipLabel,
      };
    }
    rideSubtotal = clampedMinutes * UNLIMITED_OUT_RATE;
    processingFee = rideSubtotal * UNLIMITED_PROCESSING_FEE_RATE;
    total = rideSubtotal + processingFee;
    membershipLabel =
      plan === "uofa_unlimited"
        ? "U of A Unlimited – out of Fayetteville (extra per-minute)"
        : "NWA Unlimited – out of zone (extra per-minute)";
  } else {
    rideSubtotal = clampedMinutes * BASIC_RATE_PER_MIN + BASIC_PLATFORM_FEE;
    processingFee = rideSubtotal * BASIC_PROCESSING_FEE_RATE;
    total = rideSubtotal + processingFee;
    membershipLabel = "Basic (pay per ride)";
  }

  return {
    rideSubtotal,
    processingFee,
    total,
    membershipLabel,
  };
}

async function createStripeCustomerForUser({
  stripeClient,
  uid,
  profile,
  userRef,
  reason,
  previousCustomerId = null,
}) {
  logStripeDebug("getOrCreateStripeCustomer: creating Stripe customer", {
    uid,
    reason,
    previousCustomerId,
  });

  const customer = await stripeClient.customers.create({
    email: profile?.email || undefined,
    name:
      profile?.fullName ||
      profile?.name ||
      profile?.displayName ||
      undefined,
    metadata: {
      firebaseUid: uid,
    },
  });

  await userRef.set(
    {
      stripeCustomerId: customer.id,
      stripeCustomerCreatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  logStripeDebug("getOrCreateStripeCustomer: created Stripe customer", {
    uid,
    customerId: customer.id,
    reason,
    livemode: customer?.livemode ?? null,
  });

  return {
    customerId: customer.id,
    profileRef: userRef,
    profile,
  };
}

async function getOrCreateStripeCustomer(uid) {
  const stripeClient = getStripeClient("getOrCreateStripeCustomer");
  const userRef = db.collection("users").doc(uid);
  const snap = await userRef.get();
  const profile = snap.exists ? snap.data() : null;
  const existingCustomerId = profile?.stripeCustomerId || null;

  if (existingCustomerId) {
    try {
      await stripeClient.customers.retrieve(existingCustomerId);
      return {
        customerId: existingCustomerId,
        profileRef: userRef,
        profile,
      };
    } catch (err) {
      const errorMessage =
        err?.message || err?.raw?.message || err?.raw?.message || "";
      const isMissingCustomer =
        (err?.code === "resource_missing" || err?.type === "invalid_request_error") &&
        /No such customer/i.test(errorMessage || "");

      if (!isMissingCustomer) {
        throw err;
      }

      logStripeDebug("getOrCreateStripeCustomer: repairing Stripe customer mismatch", {
        uid,
        previousCustomerId: existingCustomerId,
        errorCode: err?.code || null,
      });

      return createStripeCustomerForUser({
        stripeClient,
        uid,
        profile,
        userRef,
        reason: "missing_in_live_mode",
        previousCustomerId: existingCustomerId,
      });
    }
  }

  return createStripeCustomerForUser({
    stripeClient,
    uid,
    profile,
    userRef,
    reason: "missing_customer",
  });
}

function cloneData(input) {
  return JSON.parse(JSON.stringify(input ?? {}));
}

function removeUndefinedFields(target) {
  if (!target || typeof target !== "object") {
    return target;
  }
  Object.keys(target).forEach((key) => {
    if (target[key] === undefined) {
      delete target[key];
    }
  });
  return target;
}

function extractRideInput(data) {
  if (!data) return null;
  if (data.ride && typeof data.ride === "object") {
    return data.ride;
  }
  if (data.ridePayload && typeof data.ridePayload === "object") {
    return data.ridePayload;
  }
  if (typeof data === "object") {
    return data;
  }
  return null;
}

function resolveRideTotalCents(ride = {}) {
  const centCandidates = [
    ride.totalCents,
    ride.totalFareCents,
    ride.estimatedFareCents,
  ];
  for (const candidate of centCandidates) {
    const cents = Number(candidate);
    if (Number.isFinite(cents)) {
      return Math.round(cents);
    }
  }
  const dollarCandidates = [
    ride.total,
    ride.totalFare,
    ride.quotedTotal,
    ride.fare?.total,
  ];
  for (const candidate of dollarCandidates) {
    const dollars = Number(candidate);
    if (Number.isFinite(dollars)) {
      return Math.round(dollars * 100);
    }
  }
  return null;
}

function resolveRideDurationMinutes(ride = {}) {
  const candidates = [
    ride.estimatedDurationMinutes,
    ride.durationMinutes,
    ride.estimatedDuration,
    ride.duration,
    ride.metrics?.estimatedDurationMinutes,
    ride.metrics?.durationMinutes,
    ride.rideMetrics?.estimatedDurationMinutes,
    ride.rideMetrics?.durationMinutes,
  ];
  for (const candidate of candidates) {
    const minutes = Number(candidate);
    if (Number.isFinite(minutes) && minutes > 0) {
      return minutes;
    }
  }
  return null;
}

function normalizeRideStatus(status, poolType) {
  if (status && RIDE_STATUS_ALLOWLIST.has(status)) {
    return status;
  }
  if (poolType === "uofa") {
    return "pool_searching";
  }
  return "pending_driver";
}

function sanitizeDestinationLabel(value) {
  if (typeof value !== "string") return "";
  return value.trim().slice(0, 140);
}

// === RIDE SYNC STRIPE: START ride payload helper ===
function buildRidePayload(rideInput = {}, context = {}) {
  const payload = cloneData(rideInput);
  delete payload.createdAt;
  delete payload.updatedAt;

  payload.userId = context.uid;
  payload.startedByUserId = context.uid;
  payload.membershipType = context.membershipType;
  payload.membershipStatus = context.membershipStatus || "none";
  payload.pickupLocation =
    context.pickupLocation ||
    payload.pickupLocation ||
    payload.fromLocation ||
    null;
  payload.dropoffLocation =
    context.dropoffLocation ||
    payload.dropoffLocation ||
    payload.toLocation ||
    null;
  payload.fromLocation = payload.pickupLocation;
  payload.toLocation = payload.dropoffLocation;
  payload.toDestination = sanitizeDestinationLabel(
    payload.toDestination || payload.destination || ""
  );
  payload.destination = payload.toDestination;
  payload.isGroupRide = !!payload.isGroupRide;
  payload.maxRiders = Math.max(1, Math.min(6, payload.maxRiders || 1));
  payload.currentRiderCount = Math.min(
    payload.maxRiders,
    payload.currentRiderCount || 1
  );
  payload.poolType = payload.poolType === "uofa" ? "uofa" : null;
  payload.status = normalizeRideStatus(payload.status, payload.poolType);
  payload.paymentMethod = "stripe";
  payload.paymentStatus =
    context.amountCents > 0 ? "preauthorized" : "included";
  payload.stripeAmountCents = context.amountCents || 0;
  payload.stripeAmount = (context.amountCents || 0) / 100;
  payload.stripeCurrency = "usd";
  payload.totalCents = context.totalCents || payload.totalCents || 0;
  payload.geofenceContext = context.chargeContext || null;
  payload.inHomeZone =
    typeof payload.inHomeZone === "boolean"
      ? payload.inHomeZone
      : !!(
          context.chargeContext?.pickupInside &&
          context.chargeContext?.dropoffInside
        );
  payload.estimatedDurationMinutes =
    Number(payload.estimatedDurationMinutes) ||
    Number(context.estimatedDurationMinutes) ||
    null;

  if (!payload.pickupCode && context.pickupCode) {
    payload.pickupCode = context.pickupCode;
    payload.pickupPin = context.pickupCode;
  }
  if (!payload.dropoffCode && context.dropoffCode) {
    payload.dropoffCode = context.dropoffCode;
    payload.dropoffPin = context.dropoffCode;
  }

  return payload;
}
// === RIDE SYNC STRIPE: END ride payload helper ===

function requireAuth(context) {
  if (!context?.auth?.uid) {
    throw new functions.https.HttpsError(
      "unauthenticated",
      "Authentication required."
    );
  }
  return context.auth.uid;
}

function extractUofaRideDetails(ride = {}) {
  return {
    membershipType: ride.membershipType || ride.membership || "",
    isVerified: !!(ride.uofaVerified || ride.isUofaVerified),
    riderCount:
      ride.numRiders !== undefined
        ? ride.numRiders
        : ride.riderCount !== undefined
        ? ride.riderCount
        : 1,
    pickupCity: (ride.pickupCity || ride.city || "").toLowerCase(),
    fromLocation: ride.fromLocation || ride.pickupLocation,
    gender: normalizePoolGender(
      ride.gender ||
        ride.riderGender ||
        ride.profileGender ||
        null
    ),
  };
}

function coerceLatLng(loc) {
  if (!loc) return null;
  if (typeof loc.lat === "function" && typeof loc.lng === "function") {
    const latFn = Number(loc.lat());
    const lngFn = Number(loc.lng());
    if (Number.isFinite(latFn) && Number.isFinite(lngFn)) {
      return { lat: latFn, lng: lngFn };
    }
  }
  const latValue =
    loc.lat ?? loc.latitude ?? loc._lat ?? loc._latitude ?? loc.latDegrees;
  const lngValue =
    loc.lng ??
    loc.lon ??
    loc.longitude ??
    loc._lng ??
    loc._longitude ??
    loc.lngDegrees;
  const lat = Number(latValue);
  const lng = Number(lngValue);
  if (Number.isFinite(lat) && Number.isFinite(lng)) {
    return { lat, lng };
  }
  return null;
}

function hasValidLocation(loc) {
  return loc && typeof loc.lat === "number" && typeof loc.lng === "number";
}

function isUofaEligibleRide(details) {
  return (
    details.membershipType === "uofa_unlimited" &&
    details.isVerified &&
    details.riderCount === 1 &&
    hasValidLocation(details.fromLocation) &&
    !!details.gender
  );
}

function pickupCitiesMatch(cityA, cityB) {
  if (!cityA || !cityB) return true;
  return cityA === cityB;
}

function isRideAvailableForPooling(ride = {}) {
  const status = ride.status || "pending";
  if (ride.driverId) return false;
  if (ride.groupId) return false;
  return UOFA_PENDING_STATUSES.includes(status);
}

/**
 * Helper: calculate distance between two lat/lng points in kilometers (Haversine).
 */
function distanceKm(a, b) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const R = 6371; // Earth radius in km
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);

  const sinLat = Math.sin(dLat / 2);
  const sinLng = Math.sin(dLng / 2);

  const h =
    sinLat * sinLat +
    Math.cos(lat1) * Math.cos(lat2) * sinLng * sinLng;

  const c = 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
  return R * c;
}

// === RIDE SYNC STRIPE: START geofence helpers ===
const KM_TO_MILES = 0.621371;
const DEFAULT_UOFA_GEOFENCE = {
  center: { lat: 36.063, lng: -94.171 },
  radiusMiles: 6,
};
const DEFAULT_NWA_GEOFENCE = {
  center: {
    lat: runtimeConfig?.geo?.nwaCenter?.lat ?? 36.334,
    lng: runtimeConfig?.geo?.nwaCenter?.lng ?? -94.118,
  },
  radiusMiles: runtimeConfig?.geo?.nwaRadiusMiles ?? 30,
};
const SURCHARGE_BASE_CENTS = 500;
const SURCHARGE_PER_MILE_CENTS = 175;

function milesBetweenPoints(a, b) {
  return distanceKm(a, b) * KM_TO_MILES;
}

function isInsideCircle(lat, lng, centerLat, centerLng, radiusMiles) {
  if (
    typeof lat !== "number" ||
    typeof lng !== "number" ||
    typeof centerLat !== "number" ||
    typeof centerLng !== "number" ||
    typeof radiusMiles !== "number" ||
    radiusMiles <= 0
  ) {
    return false;
  }
  const distanceMiles = milesBetweenPoints(
    { lat, lng },
    { lat: centerLat, lng: centerLng }
  );
  return distanceMiles <= radiusMiles;
}

function resolveGeofenceForPlan(planKey) {
  if (planKey === "uofa_unlimited") {
    return DEFAULT_UOFA_GEOFENCE;
  }
  if (planKey === "nwa_unlimited") {
    return DEFAULT_NWA_GEOFENCE;
  }
  return null;
}

function maxDistanceMilesFromCenter(locations = [], geofence) {
  if (!geofence) return Infinity;
  let maxMiles = 0;
  locations.forEach((loc) => {
    if (hasValidLocation(loc)) {
      const miles = milesBetweenPoints(loc, geofence.center);
      if (miles > maxMiles) {
        maxMiles = miles;
      }
    }
  });
  return maxMiles;
}

function computeOutOfZoneSurchargeCents(totalCents, extraMiles) {
  if (!Number.isFinite(extraMiles) || extraMiles <= 0) {
    return Math.max(0, Math.round(totalCents));
  }
  const bonus = Math.round(extraMiles * SURCHARGE_PER_MILE_CENTS);
  const calculated = SURCHARGE_BASE_CENTS + bonus;
  const rideTotal = Math.max(0, Math.round(totalCents));
  return Math.min(rideTotal, Math.max(SURCHARGE_BASE_CENTS, calculated));
}

function calculateRideChargeContext({
  membershipType,
  membershipStatus,
  pickupLocation,
  dropoffLocation,
  totalCents,
}) {
  const normalizedPlan = normalizeMembershipPlan(membershipType || "basic");
  const status = (membershipStatus || "none").toLowerCase();
  const amountCents = Math.max(0, Math.round(Number(totalCents) || 0));

  if (!amountCents) {
    return {
      amountCents: 0,
      pickupInside: false,
      dropoffInside: false,
      geofenceName: null,
      surchargeCents: 0,
    };
  }

  const geofence = resolveGeofenceForPlan(normalizedPlan);
  const pickupInside =
    geofence && hasValidLocation(pickupLocation)
      ? isInsideCircle(
          pickupLocation.lat,
          pickupLocation.lng,
          geofence.center.lat,
          geofence.center.lng,
          geofence.radiusMiles
        )
      : false;
  const dropoffInside =
    geofence && hasValidLocation(dropoffLocation)
      ? isInsideCircle(
          dropoffLocation.lat,
          dropoffLocation.lng,
          geofence.center.lat,
          geofence.center.lng,
          geofence.radiusMiles
        )
      : false;

  if (!geofence || status !== "active") {
    return {
      amountCents,
      pickupInside,
      dropoffInside,
      geofenceName: null,
      surchargeCents: 0,
    };
  }

  if (pickupInside && dropoffInside) {
    return {
      amountCents: 0,
      pickupInside: true,
      dropoffInside: true,
      geofenceName: normalizedPlan,
      surchargeCents: 0,
    };
  }

  const farthestMiles = maxDistanceMilesFromCenter(
    [pickupLocation, dropoffLocation],
    geofence
  );
  const overageMiles = Math.max(0, farthestMiles - geofence.radiusMiles);
  const surchargeCents = computeOutOfZoneSurchargeCents(amountCents, overageMiles);

  return {
    amountCents: surchargeCents,
    pickupInside,
    dropoffInside,
    geofenceName: normalizedPlan,
    surchargeCents,
    overageMiles,
  };
}
// === RIDE SYNC STRIPE: END geofence helpers ===

function generateRideCode(length = 4) {
  let code = "";
  for (let i = 0; i < length; i += 1) {
    const idx = Math.floor(Math.random() * PIN_CHARSET.length);
    code += PIN_CHARSET.charAt(idx);
  }
  return code;
}

function normalizeRideCode(value) {
  if (value === null || value === undefined) return null;
  const str = String(value).trim().toUpperCase();
  return str || null;
}

function ensurePinUpdates(ride = {}, overrides = {}) {
  const updates = {};
  const overridePickup = normalizeRideCode(overrides.pickupCode);
  const overrideDropoff = normalizeRideCode(overrides.dropoffCode);
  const existingPickup =
    normalizeRideCode(ride.pickupCode) ||
    normalizeRideCode(ride.pickupPin) ||
    normalizeRideCode(ride.pickupPIN) ||
    normalizeRideCode(ride.pickup_code) ||
    normalizeRideCode(ride.boardingCode);
  const existingDropoff =
    normalizeRideCode(ride.dropoffCode) ||
    normalizeRideCode(ride.dropoffPin) ||
    normalizeRideCode(ride.dropoffPIN) ||
    normalizeRideCode(ride.dropoff_code) ||
    normalizeRideCode(ride.dropoffBoardingCode);

  if (overridePickup) {
    updates.pickupCode = overridePickup;
    updates.pickupPin = overridePickup;
  } else if (!existingPickup) {
    const code = generateRideCode();
    updates.pickupCode = code;
    updates.pickupPin = code;
  }

  if (overrideDropoff) {
    updates.dropoffCode = overrideDropoff;
    updates.dropoffPin = overrideDropoff;
  } else if (!existingDropoff) {
    const code = generateRideCode();
    updates.dropoffCode = code;
    updates.dropoffPin = code;
  }

  if (!Object.keys(updates).length) {
    return null;
  }
  updates.pinGeneratedAt = FieldValue.serverTimestamp();
  return updates;
}

/* exports.ensureRidePins = functions.firestore
 *   .document("rideRequests/{rideId}")
 *   .onCreate(async (snap, context) => {
 *     try {
 *       const ride = snap.data() || {};
 *       const updates = ensurePinUpdates(ride);
 *       if (!updates) {
 *         return null;
 *       }
 *       await snap.ref.update(updates);
 *       console.log(
 *         `[ensureRidePins] Added ride codes for ${context.params.rideId}.`
 *       );
 *       return null;
 *     } catch (err) {
 *       console.error("[ensureRidePins] Error:", err);
 *       return null;
 *     }
 *   });
 */

exports.notifyDriverOnNewRide = functions.firestore
  .document("rideRequests/{rideId}")
  .onCreate(async (snap, context) => {
    const ride = snap.data();
    const rideId = context.params.rideId;

    try {
      const status = ride.status || "pending";
      // Only notify on fresh rides that need a driver
      const notifyStatuses = ["pending_driver", "pooled_pending_driver", "pending"];
      if (!notifyStatuses.includes(status)) {
        console.log(`[notifyDriverOnNewRide] Ride ${rideId} status=${status}, skipping.`);
        return null;
      }

      // Find online drivers with FCM tokens (for now there's just you)
      const driversSnap = await db
        .collection("drivers")
        .where("isOnline", "==", true)
        .where("fcmToken", ">", "")
        .orderBy("fcmToken")
        .limit(10)
        .get();

      if (driversSnap.empty) {
        console.log("[notifyDriverOnNewRide] No online drivers with FCM tokens.");
        return null;
      }

      const tokens = [];
      const tokenDocMap = new Map();
      driversSnap.forEach((docSnap) => {
        const d = docSnap.data();
        if (d.fcmToken) {
          tokens.push(d.fcmToken);
          tokenDocMap.set(d.fcmToken, docSnap.ref);
        }
      });

      if (!tokens.length) {
        console.log("[notifyDriverOnNewRide] No tokens found.");
        return null;
      }

      const title = "New Ride";
      const body =
        ride.membershipType === "uofa_unlimited"
          ? "New U of A pooled ride is waiting."
          : "You have a new RideSync request.";

      const message = {
        notification: {
          title,
          body
        },
        data: {
          rideId: rideId,
          click_action: "https://ride-sync-nwa.web.app/driver.html"
        },
        tokens
      };

      const response = await admin.messaging().sendMulticast(message);
      console.log(
        `[notifyDriverOnNewRide] Sent notifications for ride ${rideId}. Success count: ${response.successCount}`
      );

      const invalidTokenCodes = new Set([
        "messaging/registration-token-not-registered",
        "messaging/invalid-registration-token",
      ]);

      const cleanupPromises = [];
      response.responses.forEach((resp, idx) => {
        if (!resp.success && invalidTokenCodes.has(resp.error?.code)) {
          const badToken = tokens[idx];
          const docRef = tokenDocMap.get(badToken);
          if (docRef) {
            cleanupPromises.push(
              docRef.update({
                fcmToken: FieldValue.delete(),
              })
            );
          }
        }
      });

      if (cleanupPromises.length) {
        await Promise.all(cleanupPromises);
        console.log(
          `[notifyDriverOnNewRide] Cleaned up ${cleanupPromises.length} invalid driver tokens.`
        );
      }

      return null;
    } catch (err) {
      console.error("[notifyDriverOnNewRide] Error:", err);
      return null;
    }
  });

/**
 * Auto U of A pooling:
 * - Triggered when ANY rideRequest is created.
 * - If it is U of A eligible (unlimited, verified, 1 rider),
 *   we look for another similar U of A ride:
 *   - no driver yet
 *   - no group yet
 *   - 1 rider
 *   - close by (pickup distance < 3 km)
 * - If found, we make them a group:
 *   - poolType: "uofa"
 *   - isGroupRide: true
 *   - groupId: one shared ID
 *   - currentRiderCount: 2
 *   - maxRiders: 2
 *   - status: "pooled_pending_driver"
 */
exports.uofaAutoPool = functions.firestore
  .document("rideRequests/{rideId}")
  .onCreate(async (snap, context) => {
    const newRide = snap.data();
    const newRideId = context.params.rideId;

    try {
      const newDetails = extractUofaRideDetails(newRide);

      if (!isUofaEligibleRide(newDetails)) {
        console.log(
          `[uofaAutoPool] Ride ${newRideId} not eligible for U of A pooling.`
        );
        return null;
      }

      if (newDetails.pickupCity && newDetails.pickupCity !== "fayetteville") {
        console.log(
          `[uofaAutoPool] Ride ${newRideId} not in Fayetteville (pickupCity=${newDetails.pickupCity}).`
        );
        return null;
      }

      const fromLoc = newDetails.fromLocation;
      const newGender = newDetails.gender;

      // 3) Query other U of A rides with pending-like status
      const cutoffTimestamp = Timestamp.fromMillis(
        Date.now() - CANDIDATE_LOOKBACK_MINUTES * 60 * 1000
      );
      const candidatesSnap = await db
        .collection("rideRequests")
        .where("membershipType", "==", "uofa_unlimited")
        .where("status", "in", [
          "pending_driver",
          "pool_searching",
          "pooled_pending_driver",
        ])
        .where("createdAt", ">=", cutoffTimestamp)
        .orderBy("createdAt", "desc")
        .limit(20)
        .get();

      let bestMatch = null;
      let bestDistance = Infinity;

      candidatesSnap.forEach((docSnap) => {
        const cid = docSnap.id;
        if (cid === newRideId) return;

        const data = docSnap.data();
        if (!isRideAvailableForPooling(data)) return;

        const candidateDetails = extractUofaRideDetails(data);
        if (!isUofaEligibleRide(candidateDetails)) return;
        if (!gendersCompatible(candidateDetails.gender, newGender)) return;
        if (
          !pickupCitiesMatch(
            candidateDetails.pickupCity,
            newDetails.pickupCity
          )
        ) {
          return;
        }

        const dist = distanceKm(
          { lat: fromLoc.lat, lng: fromLoc.lng },
          {
            lat: candidateDetails.fromLocation.lat,
            lng: candidateDetails.fromLocation.lng,
          }
        );

        if (dist < MAX_POOL_DISTANCE_KM && dist < bestDistance) {
          bestDistance = dist;
          bestMatch = {
            id: cid,
            data,
          };
        }
      });

      if (!bestMatch) {
        console.log(
          `[uofaAutoPool] No compatible U of A match for ride ${newRideId}.`
        );
        return null;
      }

      console.log(
        `[uofaAutoPool] Grouping ride ${newRideId} with ${bestMatch.id}, distance=${bestDistance.toFixed(
          2
        )}km`
      );

      const groupId = bestMatch.id; // use earlier ride as group host
      const newRideRef = snap.ref;
      const matchRideRef = db.collection("rideRequests").doc(bestMatch.id);

      await db.runTransaction(async (tx) => {
        const [freshNewSnap, freshMatchSnap] = await Promise.all([
          tx.get(newRideRef),
          tx.get(matchRideRef),
        ]);

        if (!freshNewSnap.exists || !freshMatchSnap.exists) {
          throw new Error("Ride document missing during pooling transaction.");
        }

        const freshNew = freshNewSnap.data();
        const freshMatch = freshMatchSnap.data();

        if (
          !isRideAvailableForPooling(freshNew) ||
          !isRideAvailableForPooling(freshMatch)
        ) {
          throw new Error(
            "Ride already assigned before transaction could complete."
          );
        }

        const freshNewDetails = extractUofaRideDetails(freshNew);
        const freshMatchDetails = extractUofaRideDetails(freshMatch);

        if (
          !isUofaEligibleRide(freshNewDetails) ||
          !isUofaEligibleRide(freshMatchDetails)
        ) {
          throw new Error("Ride no longer eligible for U of A pooling.");
        }

        if (
          !gendersCompatible(
            freshNewDetails.gender,
            freshMatchDetails.gender
          )
        ) {
          throw new Error("Ride gender mismatch discovered during transaction.");
        }

        if (
          !pickupCitiesMatch(
            freshNewDetails.pickupCity,
            freshMatchDetails.pickupCity
          )
        ) {
          throw new Error("Ride city mismatch discovered during transaction.");
        }

        const sharedPickup =
          normalizeRideCode(freshMatch.pickupCode) ||
          normalizeRideCode(freshMatch.pickupPin) ||
          normalizeRideCode(freshNew.pickupCode) ||
          normalizeRideCode(freshNew.pickupPin) ||
          generateRideCode();
        const sharedDropoff =
          normalizeRideCode(freshMatch.dropoffCode) ||
          normalizeRideCode(freshMatch.dropoffPin) ||
          normalizeRideCode(freshNew.dropoffCode) ||
          normalizeRideCode(freshNew.dropoffPin) ||
          generateRideCode();

        const updatePayload = {
          poolType: "uofa",
          isGroupRide: true,
          groupId: groupId,
          currentRiderCount: 2,
          maxRiders: 2,
          status: "pooled_pending_driver",
        };
        const sharedCodes = {
          pickupCode: sharedPickup,
          pickupPin: sharedPickup,
          dropoffCode: sharedDropoff,
          dropoffPin: sharedDropoff,
          pinGeneratedAt: FieldValue.serverTimestamp(),
        };

        tx.update(newRideRef, { ...updatePayload, ...sharedCodes });
        tx.update(matchRideRef, { ...updatePayload, ...sharedCodes });
      });

      console.log(
        `[uofaAutoPool] Created group ${groupId} with rides ${newRideId} & ${bestMatch.id}`
      );

      return null;
    } catch (err) {
      console.error("[uofaAutoPool] Error:", err);
      return null;
    }
  });

exports.createMembershipPaymentIntent = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onCall(async (data, context) => {
    const uid = requireAuth(context);
    const planKey = normalizeMembershipPlan(data?.plan);
    const planConfig = resolveMembershipPlan(planKey);
    if (!planConfig) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Unknown membership plan."
      );
    }
    if (planConfig.amountCents <= 0) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Selected plan does not require payment."
      );
    }

    const stripe = getStripeClient("createMembershipPaymentIntent");
    const { customerId } = await getOrCreateStripeCustomer(uid);

    const paymentIntent = await stripe.paymentIntents.create({
      amount: planConfig.amountCents,
      currency: planConfig.currency,
      customer: customerId,
      metadata: {
        firebaseUid: uid,
        membershipPlan: planKey,
        purpose: "membership",
      },
      automatic_payment_methods: { enabled: true },
    });

    logStripeDebug("createMembershipPaymentIntent: created PaymentIntent", {
      planKey,
      amountCents: planConfig.amountCents,
      currency: planConfig.currency,
      paymentIntentId: paymentIntent.id,
      livemode: paymentIntent.livemode,
    });

    await db
      .collection("pendingMembershipPayments")
      .doc(paymentIntent.id)
      .set({
        userId: uid,
        plan: planKey,
        amountCents: planConfig.amountCents,
        currency: planConfig.currency,
        createdAt: FieldValue.serverTimestamp(),
      });

    return {
      clientSecret: paymentIntent.client_secret,
      paymentIntentId: paymentIntent.id,
      amountCents: planConfig.amountCents,
      currency: planConfig.currency,
      planLabel: planConfig.label,
      plan: planKey,
      livemode: paymentIntent?.livemode ?? null,
    };
  });

async function applyMembershipPlanHandler(data = {}, uid) {
  const planKey = normalizeMembershipPlan(data?.plan);
  const planConfig = resolveMembershipPlan(planKey);
  if (!planConfig) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "Unknown membership plan."
    );
  }

  const userRef = db.collection("users").doc(uid);
  const membershipDurationDays =
    Number.isFinite(Number(planConfig.durationDays)) && planConfig.durationDays > 0
      ? Number(planConfig.durationDays)
      : MEMBERSHIP_DEFAULT_DURATION_DAYS;
  const membershipExpiresAt =
    planKey === "basic"
      ? null
      : computeMembershipExpirationTimestamp(membershipDurationDays);

  if (planKey === "basic") {
    await userRef.set(
      {
        membershipType: "basic",
        membershipStatus: "none",
        membershipExpiresAt: FieldValue.delete(),
        membershipExpiredAt: FieldValue.delete(),
        stripeSubscriptionId: FieldValue.delete(),
        pendingMembershipPlanId: FieldValue.delete(),
        pendingSubscriptionId: FieldValue.delete(),
      },
      { merge: true }
    );
    return { status: "updated" };
  }

  const userSnap = await userRef.get();
  const profileData = userSnap.exists ? userSnap.data() : {};
  const needsApproval =
    planKey === "uofa_unlimited" && !profileData?.uofaVerified;
  const membershipStatus = needsApproval ? "pending_verification" : "active";

  if (planConfig.amountCents === 0) {
    await userRef.set(
      {
        membershipType: planKey || "basic",
        membershipStatus,
        membershipRenewedAt: FieldValue.serverTimestamp(),
        membershipExpiresAt: membershipExpiresAt || FieldValue.delete(),
        membershipApprovalRequired: needsApproval || FieldValue.delete(),
        membershipExpiredAt: FieldValue.delete(),
      },
      { merge: true }
    );
    return { status: "updated" };
  }

  const paymentIntentId = data?.paymentIntentId;
  if (!paymentIntentId) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "Payment confirmation is required for this membership."
    );
  }

  const pendingRef = db
    .collection("pendingMembershipPayments")
    .doc(paymentIntentId);
  const pendingSnap = await pendingRef.get();
  if (!pendingSnap.exists) {
    throw new functions.https.HttpsError(
      "not-found",
      "Membership payment not found."
    );
  }
  const pending = pendingSnap.data();
  if (pending.userId !== uid) {
    throw new functions.https.HttpsError(
      "permission-denied",
      "Cannot apply a payment that belongs to a different user."
    );
  }
  if (pending.plan !== planKey) {
    throw new functions.https.HttpsError(
      "failed-precondition",
      "Payment plan does not match the requested membership."
    );
  }

  const stripe = getStripeClient("applyMembershipPlanHandler");
  const intent = await stripe.paymentIntents.retrieve(paymentIntentId);
  if (intent.status !== "succeeded") {
    throw new functions.https.HttpsError(
      "failed-precondition",
      "Membership payment has not completed."
    );
  }

  await userRef.set(
    {
      membershipType: planKey,
      membershipStatus,
      membershipRenewedAt: FieldValue.serverTimestamp(),
      membershipExpiresAt: membershipExpiresAt || FieldValue.delete(),
      membershipPaidAmountCents: pending.amountCents,
      membershipPaidCurrency: pending.currency,
      membershipStripePaymentIntentId: paymentIntentId,
      membershipApprovalRequired: needsApproval || FieldValue.delete(),
      membershipExpiredAt: FieldValue.delete(),
    },
    { merge: true }
  );

  await pendingRef.delete();

  return { status: "updated" };
}

exports.applyMembershipPlan = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onCall(async (data, context) => {
    const uid = requireAuth(context);
    return applyMembershipPlanHandler(data, uid);
  });

const APPLY_MEMBERSHIP_ALLOWED_ORIGINS = new Set([
  "https://ride-sync-nwa.web.app",
  "https://ride-sync-nwa.firebaseapp.com",
  "http://localhost:5000",
  "http://localhost:5173",
  "http://127.0.0.1:5000",
]);

function setApplyMembershipCorsHeaders(req, res) {
  const origin = req.headers.origin;
  if (origin && APPLY_MEMBERSHIP_ALLOWED_ORIGINS.has(origin)) {
    res.set("Access-Control-Allow-Origin", origin);
    res.set("Vary", "Origin");
    res.set("Access-Control-Allow-Credentials", "true");
  }
  res.set(
    "Access-Control-Allow-Headers",
    "Authorization, Content-Type, X-Requested-With"
  );
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
}

async function extractUidFromRequest(req) {
  const authHeader = req.headers.authorization || "";
  if (!authHeader.startsWith("Bearer ")) {
    throw new functions.https.HttpsError(
      "unauthenticated",
      "Authentication token missing."
    );
  }
  const token = authHeader.slice("Bearer ".length).trim();
  if (!token) {
    throw new functions.https.HttpsError(
      "unauthenticated",
      "Authentication token missing."
    );
  }
  const decoded = await admin.auth().verifyIdToken(token);
  if (!decoded?.uid) {
    throw new functions.https.HttpsError(
      "unauthenticated",
      "Invalid authentication token."
    );
  }
  return decoded.uid;
}

exports.applyMembershipPlanHttp = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onRequest(async (req, res) => {
    setApplyMembershipCorsHeaders(req, res);
    if (req.method === "OPTIONS") {
      res.status(204).send("");
      return;
    }
    if (req.method !== "POST") {
      res.status(405).json({
        error: {
          code: "method-not-allowed",
          message: "Use POST for this endpoint.",
        },
      });
      return;
    }

    try {
      const uid = await extractUidFromRequest(req);
      const payload = typeof req.body === "object" && req.body !== null ? req.body : {};
      const result = await applyMembershipPlanHandler(payload, uid);
      res.status(200).json(result);
    } catch (err) {
      console.error("[applyMembershipPlanHttp] Error:", err);
      if (err instanceof functions.https.HttpsError) {
        res.status(err.httpErrorCode.status).json({
          error: { code: err.code, message: err.message },
        });
        return;
      }
      res.status(500).json({
        error: {
          code: "internal",
          message: "Failed to update membership. Try again shortly.",
        },
      });
    }
  });

// === RIDE SYNC STRIPE: START createMembershipSubscriptionIntent ===
exports.createMembershipSubscriptionIntent = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onCall(async (data, context) => {
    const uid = requireAuth(context);
    const planId = normalizeMembershipPlan(data?.planId || data?.plan);
    if (!["uofa_unlimited", "nwa_unlimited"].includes(planId)) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Unsupported membership plan."
      );
    }
    hydrateStripeSettingsFromSecrets();
    const priceId = planId === "uofa_unlimited" ? uofaPriceId : nwaPriceId;
    if (!priceId) {
      logStripeConfigState("createMembershipSubscriptionIntent", {
        severity: "error",
        includeEnvKeys: true,
        extra: { planId, hasStripeSecret: !!stripeSecretKey },
      });
      throw new functions.https.HttpsError(
        "unavailable",
        "Membership billing is temporarily unavailable. Please contact RideSync support."
      );
    }

    const stripeClient = getStripeClient(
      "createMembershipSubscriptionIntent"
    );
    const { customerId, profileRef } = await getOrCreateStripeCustomer(uid);
    const userRef = profileRef || db.collection("users").doc(uid);

    const subscription = await stripeClient.subscriptions.create({
      customer: customerId,
      items: [{ price: priceId }],
      payment_behavior: "default_incomplete",
      payment_settings: { save_default_payment_method: "on_subscription" },
      expand: ["latest_invoice.payment_intent"],
      metadata: {
        firebaseUid: uid,
        planId,
      },
    });

    const invoicePaymentIntent = subscription?.latest_invoice?.payment_intent;
    const clientSecret = invoicePaymentIntent?.client_secret;
    if (!clientSecret) {
      throw new functions.https.HttpsError(
        "internal",
        "Unable to start the Stripe subscription."
      );
    }

    await userRef.set(
      {
        pendingMembershipPlanId: planId,
        pendingSubscriptionId: subscription.id,
        stripeCustomerId: customerId,
        membershipUpgradeStartedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    return {
      clientSecret,
      subscriptionId: subscription.id,
      planId,
      livemode: invoicePaymentIntent?.livemode ?? null,
    };
  });
// === RIDE SYNC STRIPE: END createMembershipSubscriptionIntent ===

// === RIDE SYNC STRIPE: START finalizeMembershipSubscription ===
exports.finalizeMembershipSubscription = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onCall(async (data, context) => {
    const uid = requireAuth(context);
    const subscriptionId = data?.subscriptionId;
    if (!subscriptionId) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Stripe subscription ID is required."
      );
    }

    const stripeClient = getStripeClient("finalizeMembershipSubscription");
    const subscription = await stripeClient.subscriptions.retrieve(
      subscriptionId,
      {
        expand: ["latest_invoice.payment_intent", "items.data.price.product"],
      }
    );
    if (!subscription) {
      throw new functions.https.HttpsError(
        "not-found",
        "Subscription not found."
      );
    }
    if (
      subscription.metadata?.firebaseUid &&
      subscription.metadata.firebaseUid !== uid
    ) {
      throw new functions.https.HttpsError(
        "permission-denied",
        "This subscription belongs to another user."
      );
    }

    const status = subscription.status;
    if (status !== "active" && status !== "trialing") {
      return { status };
    }

    const subscriptionPlan =
      normalizeMembershipPlan(subscription.metadata?.planId) ||
      normalizeMembershipPlan(
        subscription.items?.data?.[0]?.price?.lookup_key ||
          subscription.items?.data?.[0]?.price?.nickname
      );
    if (!["uofa_unlimited", "nwa_unlimited"].includes(subscriptionPlan)) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Subscription is missing a supported plan."
      );
    }

    const membershipStatus =
      subscriptionPlan === "uofa_unlimited"
        ? "pending_verification"
        : "active";
    const subscriptionPeriodEnd =
      typeof subscription.current_period_end === "number"
        ? subscription.current_period_end
        : Number(subscription.current_period_end);
    const membershipExpiresAt =
      Number.isFinite(subscriptionPeriodEnd) && subscriptionPeriodEnd > 0
        ? Timestamp.fromMillis(subscriptionPeriodEnd * 1000)
        : computeMembershipExpirationTimestamp();
    const customerId =
      typeof subscription.customer === "string"
        ? subscription.customer
        : subscription.customer?.id;
    const userRef = db.collection("users").doc(uid);
    const membershipHistoryRef = userRef
      .collection("membershipHistory")
      .doc(subscription.id);

    await Promise.all([
      userRef.set(
        {
          membershipType: subscriptionPlan,
          membershipStatus,
          stripeSubscriptionId: subscription.id,
          stripeCustomerId: customerId || FieldValue.delete(),
          pendingMembershipPlanId: FieldValue.delete(),
          pendingSubscriptionId: FieldValue.delete(),
          membershipRenewedAt: FieldValue.serverTimestamp(),
          membershipExpiresAt: membershipExpiresAt || FieldValue.delete(),
          membershipExpiredAt: FieldValue.delete(),
        },
        { merge: true }
      ),
      membershipHistoryRef.set(
        {
          processedAt: FieldValue.serverTimestamp(),
          planId: subscriptionPlan,
          subscriptionId,
          status,
          invoiceId: subscription.latest_invoice?.id || null,
        },
        { merge: true }
      ),
    ]);

    return { status: "active", membershipType: subscriptionPlan };
  });
// === RIDE SYNC STRIPE: END finalizeMembershipSubscription ===

function sanitizeCheckoutDescription(value) {
  if (typeof value !== "string") {
    return "RideSync ride fare";
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return "RideSync ride fare";
  }
  return trimmed.slice(0, 140);
}

function resolveCheckoutAmountCents(payload = {}) {
  const amountInput =
    typeof payload.amountCents === "number"
      ? payload.amountCents
      : typeof payload.amountCents === "string"
      ? Number(payload.amountCents)
      : typeof payload.amount_cents === "number"
      ? payload.amount_cents
      : typeof payload.amount_cents === "string"
      ? Number(payload.amount_cents)
      : null;
  const amountCents = Number.isFinite(amountInput)
    ? Math.round(amountInput)
    : null;
  if (!amountCents || amountCents <= 0) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "amountCents must be greater than zero."
    );
  }
  return amountCents;
}

const DEFAULT_CHECKOUT_BASE_URL =
  process.env.CHECKOUT_BASE_URL ||
  runtimeConfig?.app?.checkout_base_url ||
  "https://ride-sync-nwa.web.app";

function sanitizeRedirectBaseUrl(value) {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  try {
    const url = new URL(trimmed);
    const protocol = url.protocol.toLowerCase();
    const hostname = url.hostname.toLowerCase();
    const isLocalhost =
      hostname === "localhost" ||
      hostname === "127.0.0.1" ||
      hostname === "::1";
    const isAllowedProtocol =
      protocol === "https:" || (protocol === "http:" && isLocalhost);
    if (!isAllowedProtocol) {
      return null;
    }
    return url.origin.replace(/\/+$/, "");
  } catch (err) {
    return null;
  }
}

function resolveCheckoutRedirectBaseUrl({
  payload = {},
  requestOrigin = null,
  requestReferer = null,
} = {}) {
  const payloadUrl =
    payload.redirectBaseUrl || payload.redirect_base_url || null;
  const candidates = [
    payloadUrl,
    requestOrigin,
    requestReferer,
    DEFAULT_CHECKOUT_BASE_URL,
  ];
  for (const candidate of candidates) {
    const sanitized = sanitizeRedirectBaseUrl(candidate);
    if (sanitized) {
      return sanitized;
    }
  }
  return "https://ride-sync-nwa.web.app";
}

async function createRideCheckoutSessionInternal({
  payload = {},
  uid,
  email,
  requestOrigin = null,
  requestReferer = null,
}) {
  if (!uid) {
    throw new functions.https.HttpsError(
      "unauthenticated",
      "Sign in to start checkout."
    );
  }

  const amountCents = resolveCheckoutAmountCents(payload);
  const rideInput = extractRideInput(payload.ridePayload || payload.ride);
  if (!rideInput) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "Ride payload is required."
    );
  }

  const totalCentsResolved = resolveRideTotalCents(rideInput);
  if (!Number.isFinite(totalCentsResolved) || totalCentsResolved <= 0) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "Estimated fare (totalCents) is required."
    );
  }
  const totalCents = Math.max(0, Math.round(totalCentsResolved));

  const pickupLocation =
    coerceLatLng(rideInput.pickupLocation) ||
    coerceLatLng(rideInput.fromLocation) ||
    coerceLatLng(rideInput.currentLocation);
  const dropoffLocation =
    coerceLatLng(rideInput.dropoffLocation) ||
    coerceLatLng(rideInput.toLocation) ||
    coerceLatLng(rideInput.destinationLocation);
  if (!hasValidLocation(pickupLocation) || !hasValidLocation(dropoffLocation)) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "Pickup and dropoff coordinates are required."
    );
  }

  const estimatedMinutes = resolveRideDurationMinutes(rideInput);
  if (!Number.isFinite(estimatedMinutes) || estimatedMinutes <= 0) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "Estimated ride duration is required."
    );
  }

  const stripeClient = getStripeClient("createRideCheckoutSessionInternal");
  const userRef = db.collection("users").doc(uid);
  const userSnap = await userRef.get();
  let profile = userSnap.exists ? userSnap.data() : {};
  profile = await maybeDowngradeExpiredMembership(userRef, profile);
  const membershipType = normalizeMembershipPlan(
    profile?.membershipType || profile?.membership || "basic"
  );
  const membershipStatus = profile?.membershipStatus || "none";

  const chargeContext = calculateRideChargeContext({
    membershipType,
    membershipStatus,
    pickupLocation,
    dropoffLocation,
    totalCents,
  });

  if (!chargeContext.amountCents || chargeContext.amountCents <= 0) {
    throw new functions.https.HttpsError(
      "failed-precondition",
      "This ride does not require a Stripe payment."
    );
  }

  if (chargeContext.amountCents !== amountCents) {
    logStripeDebug("createRideCheckoutSession: amount mismatch detected", {
      uid,
      amountCentsClient: amountCents,
      amountCentsServer: chargeContext.amountCents,
    });
  }

  const sanitizedPayload = buildRidePayload(rideInput, {
    uid,
    membershipType,
    membershipStatus,
    pickupLocation,
    dropoffLocation,
    amountCents: chargeContext.amountCents,
    totalCents,
    chargeContext,
    estimatedDurationMinutes: estimatedMinutes,
  });

  const description = sanitizeCheckoutDescription(
    payload.description ||
      sanitizedPayload.description ||
      sanitizedPayload.toDestination ||
      "RideSync ride fare"
  );

  let pendingRef = null;
  let session = null;

  try {
    const redirectBaseUrl = resolveCheckoutRedirectBaseUrl({
      payload,
      requestOrigin,
      requestReferer,
    });

    pendingRef = db.collection("pendingRidePayments").doc();
    await pendingRef.set({
      userId: uid,
      pendingId: pendingRef.id,
      ridePayload: sanitizedPayload,
      amountCents: chargeContext.amountCents,
      totalCents,
      geofenceContext: chargeContext,
      currency: "usd",
      checkoutMode: "stripe_checkout",
      createdAt: FieldValue.serverTimestamp(),
    });

    session = await stripeClient.checkout.sessions.create({
      mode: "payment",
      line_items: [
        {
          price_data: {
            currency: "usd",
            product_data: {
              name: description,
            },
            unit_amount: chargeContext.amountCents,
          },
          quantity: 1,
        },
      ],
      success_url: `${redirectBaseUrl}/payment-success?pending_id=${pendingRef.id}&session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${redirectBaseUrl}/payment-cancelled?pending_id=${pendingRef.id}`,
      customer_email: email || undefined,
      client_reference_id: pendingRef.id,
      metadata: {
        firebaseUid: uid,
        pendingRideId: pendingRef.id,
        purpose: "ride",
      },
      payment_intent_data: {
        metadata: {
          firebaseUid: uid,
          pendingRideId: pendingRef.id,
          purpose: "ride",
        },
      },
    });

    await pendingRef.update({
      stripeCheckoutSessionId: session.id,
      checkoutSessionCreatedAt: FieldValue.serverTimestamp(),
    });

    return {
      url: session.url,
      pendingId: pendingRef.id,
    };
  } catch (err) {
    if (pendingRef && !session) {
      try {
        await pendingRef.delete();
      } catch (_) {
        // best-effort cleanup
      }
    }
    throw err;
  }
}

async function maybeResolveUserFromAuthHeader(req) {
  const authHeader = req.headers.authorization || "";
  if (!authHeader.startsWith("Bearer ")) {
    return { uid: null, email: null };
  }
  const token = authHeader.slice("Bearer ".length).trim();
  if (!token) {
    return { uid: null, email: null };
  }
  try {
    const decoded = await admin.auth().verifyIdToken(token);
    const uid = decoded?.uid || null;
    const decodedEmail = decoded?.email || null;
    if (!uid) {
      return { uid: null, email: decodedEmail || null };
    }
    if (decodedEmail) {
      return { uid, email: decodedEmail };
    }
    const userRecord = await admin.auth().getUser(uid);
    return { uid, email: userRecord?.email || null };
  } catch (err) {
    logStripeDebug("maybeResolveUserFromAuthHeader: token verification failed", {
      errorCode: err?.code || err?.errorInfo?.code || null,
    });
    return { uid: null, email: null };
  }
}

exports.createRideCheckoutSession = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onRequest(async (req, res) => {
    setApplyMembershipCorsHeaders(req, res);
    if (req.method === "OPTIONS") {
      res.status(204).send("");
      return;
    }
    if (req.method !== "POST") {
      res.status(405).json({
        error: {
          code: "method-not-allowed",
          message: "Use POST for this endpoint.",
        },
      });
      return;
    }

    const payload =
      typeof req.body === "object" && req.body !== null ? req.body : {};

    try {
      const { uid, email } = await maybeResolveUserFromAuthHeader(req);
      if (!uid) {
        res.status(401).json({
          error: {
            code: "unauthenticated",
            message: "Sign in to start checkout.",
          },
        });
        return;
      }

      const result = await createRideCheckoutSessionInternal({
        payload,
        uid,
        email,
        requestOrigin: req.headers.origin || null,
        requestReferer: req.headers.referer || req.headers.referrer || null,
      });

      res.status(200).json(result);
    } catch (err) {
      if (err instanceof functions.https.HttpsError) {
        res.status(err.httpErrorCode?.status || 500).json({
          error: {
            code: err.code,
            message: err.message,
          },
        });
        return;
      }
      console.error(
        "[RideSync][Stripe] createRideCheckoutSession error",
        err
      );
      res.status(500).json({
        error: {
          code: "internal",
          message: "Unable to start checkout. Try again shortly.",
        },
      });
    }
  });

exports.createRideCheckoutSessionCallable = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onCall(async (data, context) => {
    const uid = requireAuth(context);
    const payload =
      typeof data === "object" && data !== null ? data : {};
    try {
      return await createRideCheckoutSessionInternal({
        payload,
        uid,
        email: context?.auth?.token?.email || null,
      });
    } catch (err) {
      if (err instanceof functions.https.HttpsError) {
        throw err;
      }
      console.error(
        "[RideSync][Stripe] createRideCheckoutSessionCallable error",
        err
      );
      throw new functions.https.HttpsError(
        "internal",
        "Unable to start checkout. Try again shortly."
      );
    }
  });

// === RIDE SYNC STRIPE: START createRidePaymentIntent ===
exports.createRidePaymentIntent = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onCall(async (data = {}, context) => {
    const uid = requireAuth(context);
    const amountRaw = data?.amount ?? data?.amountCents ?? data?.amount_cents;
    const amount = Number(amountRaw);
    if (!Number.isFinite(amount) || amount <= 0) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "A positive amount (in cents) is required."
      );
    }
    const normalizedAmount = Math.round(amount);

    const tipRaw =
      data?.tipAmountCents ??
      data?.tipAmount ??
      data?.tip_cents ??
      data?.tip;
    let tipAmountCents = 0;
    if (
      tipRaw !== undefined &&
      tipRaw !== null &&
      tipRaw !== "" &&
      Number.isFinite(Number(tipRaw))
    ) {
      tipAmountCents = Math.max(0, Math.round(Number(tipRaw)));
    }
    if (tipAmountCents > 20000) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Tip amount is too large."
      );
    }

    const currencyInput =
      typeof data?.currency === "string" && data.currency.trim()
        ? data.currency.trim().toLowerCase()
        : "usd";
    const rideId =
      typeof data?.rideId === "string"
        ? data.rideId
        : typeof data?.ride_id === "string"
        ? data.ride_id
        : "";
    if (!rideId) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Ride ID is required."
      );
    }

    const rideRef = db.collection("rideRequests").doc(rideId);
    const rideSnap = await rideRef.get();
    if (!rideSnap.exists) {
      throw new functions.https.HttpsError(
        "not-found",
        "Ride not found for payment."
      );
    }
    const rideData = rideSnap.data() || {};
    if (rideData.userId && rideData.userId !== uid) {
      throw new functions.https.HttpsError(
        "permission-denied",
        "Cannot pay for another rider's fare."
      );
    }

    const expectedAmountRaw = Number(rideData.stripeAmountCents);
    const expectedAmountCents = Math.round(expectedAmountRaw);
    if (!Number.isFinite(expectedAmountCents) || expectedAmountCents <= 0) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Unable to determine ride fare for payment."
      );
    }
    const totalAmountCents = expectedAmountCents + tipAmountCents;
    if (normalizedAmount !== totalAmountCents) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Ride fare mismatch. Refresh and try again."
      );
    }

    const currencyToCharge =
      typeof rideData.stripeCurrency === "string" &&
      rideData.stripeCurrency.trim()
        ? rideData.stripeCurrency.trim().toLowerCase()
        : currencyInput;

    const stripeClient = getStripeClient("createRidePaymentIntent");

    try {
      const paymentIntent = await stripeClient.paymentIntents.create({
        amount: totalAmountCents,
        currency: currencyToCharge,
        automatic_payment_methods: { enabled: true },
        metadata: {
          rideId,
          userId: uid,
          type: "ride_fare",
          base_amount_cents: expectedAmountCents,
          tip_amount_cents: tipAmountCents,
        },
      });

      return {
        clientSecret: paymentIntent.client_secret,
        amountCents: totalAmountCents,
        baseAmountCents: expectedAmountCents,
        tipAmountCents,
        livemode: paymentIntent?.livemode ?? null,
      };
    } catch (err) {
      console.error("createRidePaymentIntent error", err);
      throw new functions.https.HttpsError(
        "internal",
        err?.message || "Failed to create ride payment intent."
      );
    }
  });
// === RIDE SYNC STRIPE: END createRidePaymentIntent ===

async function finalizePendingRide({
  pendingRef,
  pending,
  uid,
  paymentIntentId,
}) {
  const rideData = cloneData(pending.ridePayload || {});
  rideData.userId = uid;
  rideData.startedByUserId = rideData.startedByUserId || uid;
  rideData.status = normalizeRideStatus(rideData.status, rideData.poolType);
  rideData.createdAt = FieldValue.serverTimestamp();
  rideData.paymentStatus = "paid";
  rideData.paymentMethod = "stripe";
  rideData.stripePaymentIntentId = paymentIntentId;
  rideData.stripeAmount = pending.amountCents / 100;
  rideData.stripeAmountCents = pending.amountCents;
  rideData.stripeCurrency = pending.currency || "usd";
  rideData.totalCents = pending.totalCents ?? rideData.totalCents ?? null;
  rideData.geofenceContext =
    pending.geofenceContext ?? rideData.geofenceContext ?? null;
  rideData.updatedAt = FieldValue.serverTimestamp();
  rideData.currentRiderCount = Math.min(
    rideData.maxRiders || 1,
    rideData.currentRiderCount || 1
  );
  if (rideData.isGroupRide && !rideData.groupId) {
    rideData.groupId = undefined;
  }

  const rideRef = db.collection("rideRequests").doc();
  if (rideData.isGroupRide && !rideData.groupId) {
    rideData.groupId = rideRef.id;
  }

  await db.runTransaction(async (tx) => {
    const freshSnap = await tx.get(pendingRef);
    if (!freshSnap.exists) {
      throw new functions.https.HttpsError(
        "not-found",
        "Pending ride payment not found."
      );
    }
    if (freshSnap.data().processedAt) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "This pending ride payment was already processed."
      );
    }
    tx.set(rideRef, rideData);
    tx.update(pendingRef, {
      processedAt: FieldValue.serverTimestamp(),
      rideId: rideRef.id,
      stripePaymentIntentId: paymentIntentId,
    });
  });

  return rideRef.id;
}

exports.finalizeRidePayment = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onCall(async (data, context) => {
    const uid = requireAuth(context);
    const pendingId = data?.pendingId;
    const paymentIntentId = data?.paymentIntentId;
    if (!pendingId || !paymentIntentId) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Pending ride reference and payment intent are required."
      );
    }

    const pendingRef = db.collection("pendingRidePayments").doc(pendingId);
    const pendingSnap = await pendingRef.get();
    if (!pendingSnap.exists) {
      throw new functions.https.HttpsError(
        "not-found",
        "Pending ride payment not found."
      );
    }
    const pending = pendingSnap.data();
    if (pending.userId !== uid) {
      throw new functions.https.HttpsError(
        "permission-denied",
        "Cannot finalize another rider's payment."
      );
    }
    if (pending.stripePaymentIntentId !== paymentIntentId) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Payment intent mismatch."
      );
    }

    const stripe = getStripeClient("finalizeRidePayment");
    const intent = await stripe.paymentIntents.retrieve(paymentIntentId);
    if (intent.status !== "succeeded") {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Ride payment has not completed."
      );
    }

    const rideId = await finalizePendingRide({
      pendingRef,
      pending,
      uid,
      paymentIntentId: intent.id,
    });

    return { rideId };
  });

exports.finalizeRideCheckoutSession = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onRequest(async (req, res) => {
    setApplyMembershipCorsHeaders(req, res);
    if (req.method === "OPTIONS") {
      res.status(204).send("");
      return;
    }
    if (req.method !== "POST") {
      res.status(405).json({
        error: {
          code: "method-not-allowed",
          message: "Use POST for this endpoint.",
        },
      });
      return;
    }

    const payload =
      typeof req.body === "object" && req.body !== null ? req.body : {};
    const pendingId =
      typeof payload.pendingId === "string"
        ? payload.pendingId
        : typeof payload.pending_id === "string"
        ? payload.pending_id
        : null;
    const sessionId =
      typeof payload.sessionId === "string"
        ? payload.sessionId
        : typeof payload.session_id === "string"
        ? payload.session_id
        : null;

    const { uid } = await maybeResolveUserFromAuthHeader(req);
    if (!uid) {
      res.status(401).json({
        error: {
          code: "unauthenticated",
          message: "Sign in to finish your ride.",
        },
      });
      return;
    }

    if (!pendingId || !sessionId) {
      res.status(400).json({
        error: {
          code: "invalid-argument",
          message: "pendingId and sessionId are required.",
        },
      });
      return;
    }

    try {
      const pendingRef = db.collection("pendingRidePayments").doc(pendingId);
      const pendingSnap = await pendingRef.get();
      if (!pendingSnap.exists) {
        res.status(404).json({
          error: {
            code: "not-found",
            message: "Pending ride payment not found.",
          },
        });
        return;
      }

      const pending = pendingSnap.data();
      if (pending.userId !== uid) {
        res.status(403).json({
          error: {
            code: "permission-denied",
            message: "Cannot finalize another rider's payment.",
          },
        });
        return;
      }

      if (pending.processedAt && pending.rideId) {
        res.status(200).json({ rideId: pending.rideId });
        return;
      }

      const stripeClient = getStripeClient("finalizeRideCheckoutSession");
      let session;
      try {
        session = await stripeClient.checkout.sessions.retrieve(sessionId, {
          expand: ["payment_intent"],
        });
      } catch (err) {
        if (err?.code === "resource_missing") {
          res.status(404).json({
            error: {
              code: "not-found",
              message: "Stripe checkout session not found.",
            },
          });
          return;
        }
        throw err;
      }

      if (!session) {
        res.status(404).json({
          error: {
            code: "not-found",
            message: "Stripe checkout session not found.",
          },
        });
        return;
      }

      if (session.client_reference_id && session.client_reference_id !== pendingId) {
        res.status(409).json({
          error: {
            code: "failed-precondition",
            message: "Session does not match the pending ride.",
          },
        });
        return;
      }

      if (
        session.metadata?.pendingRideId &&
        session.metadata.pendingRideId !== pendingId
      ) {
        res.status(409).json({
          error: {
            code: "failed-precondition",
            message: "Session metadata does not match the pending ride.",
          },
        });
        return;
      }

      if (
        pending.stripeCheckoutSessionId &&
        pending.stripeCheckoutSessionId !== session.id
      ) {
        res.status(409).json({
          error: {
            code: "failed-precondition",
            message: "Checkout session mismatch.",
          },
        });
        return;
      }

      const paymentIntentId =
        typeof session.payment_intent === "string"
          ? session.payment_intent
          : session.payment_intent?.id;
      if (!paymentIntentId) {
        res.status(409).json({
          error: {
            code: "failed-precondition",
            message: "Checkout session has not created a payment intent yet.",
          },
        });
        return;
      }

      const intent =
        typeof session.payment_intent === "object"
          ? session.payment_intent
          : await stripeClient.paymentIntents.retrieve(paymentIntentId);

      if (
        pending.stripePaymentIntentId &&
        pending.stripePaymentIntentId !== intent.id
      ) {
        res.status(409).json({
          error: {
            code: "failed-precondition",
            message: "Payment intent mismatch.",
          },
        });
        return;
      }

      if (intent.status !== "succeeded") {
        res.status(409).json({
          error: {
            code: "failed-precondition",
            message: "Stripe has not confirmed this payment yet.",
          },
        });
        return;
      }

      if (!pending.stripeCheckoutSessionId) {
        await pendingRef.update({
          stripeCheckoutSessionId: session.id,
        });
      }

      const rideId = await finalizePendingRide({
        pendingRef,
        pending,
        uid,
        paymentIntentId: intent.id,
      });

      res.status(200).json({ rideId });
    } catch (err) {
      console.error(
        "[RideSync][Stripe] finalizeRideCheckoutSession error",
        err
      );
      res.status(500).json({
        error: {
          code: "internal",
          message:
            "We received your payment but could not finalize the ride yet. Contact RideSync support.",
        },
      });
    }
  });

function sanitizeProfileString(value, maxLength = 140) {
  if (typeof value !== "string") {
    return "";
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return "";
  }
  return trimmed.slice(0, maxLength);
}

function sanitizePhoneNumber(value) {
  if (!value) {
    return "";
  }
  const digits = String(value).replace(/\D+/g, "");
  if (!digits) {
    return "";
  }
  return digits.slice(-15);
}

function normalizeBooleanInput(value) {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "string") {
    return value.trim().toLowerCase() === "true";
  }
  return Boolean(value);
}

function normalizeExtraStopsInput(stops = []) {
  if (!Array.isArray(stops)) {
    return [];
  }
  const normalized = [];
  stops.forEach((stop, index) => {
    if (!stop) return;
    const label = sanitizeProfileString(stop.label, 80);
    const location = coerceLatLng(stop.location);
    if (!label || !location) {
      return;
    }
    normalized.push({
      order: Number.isFinite(stop.order) ? stop.order : index + 1,
      label,
      location,
    });
  });
  return normalized.slice(0, MAX_EXTRA_STOPS);
}

async function loadUserProfileOrThrow(uid) {
  const userRef = db.collection("users").doc(uid);
  const snap = await userRef.get();
  if (!snap.exists) {
    throw new functions.https.HttpsError(
      "failed-precondition",
      "Complete your rider profile before requesting a ride."
    );
  }
  const initialProfile = snap.data() || {};
  const profile = await maybeDowngradeExpiredMembership(userRef, initialProfile);
  return { userRef, profile };
}

async function enforceRideCooldown(uid, planKey) {
  const normalizedPlan = normalizeMembershipPlan(planKey || "basic");
  if (!COOLDOWN_ELIGIBLE_PLANS.has(normalizedPlan)) {
    return;
  }
  const cooldownMinutes = SURGE_MODE
    ? SURGE_COOLDOWN_MINUTES
    : UNLIMITED_COOLDOWN_MINUTES;
  if (!cooldownMinutes || cooldownMinutes <= 0) {
    return;
  }
  const ridesSnap = await db
    .collection("rideRequests")
    .where("userId", "==", uid)
    .orderBy("createdAt", "desc")
    .limit(10)
    .get();
  if (ridesSnap.empty) {
    return;
  }
  let lastCompleted = null;
  ridesSnap.forEach((docSnap) => {
    if (lastCompleted) return;
    const data = docSnap.data() || {};
    if (
      (data.status === "completed" || data.status === "dropoff_code_verified") &&
      normalizeMembershipPlan(data.membershipType) === normalizedPlan &&
      data.completedAt &&
      typeof data.completedAt.toDate === "function"
    ) {
      lastCompleted = data;
    }
  });
  if (!lastCompleted) {
    return;
  }
  const completedAt =
    typeof lastCompleted.completedAt.toDate === "function"
      ? lastCompleted.completedAt.toDate()
      : null;
  if (!completedAt) {
    return;
  }
  const diffMs = Date.now() - completedAt.getTime();
  const diffMinutes = diffMs / 60000;
  const remaining = Math.ceil(cooldownMinutes - diffMinutes);
  if (remaining > 0) {
    throw new functions.https.HttpsError(
      "failed-precondition",
      `Cooldown active. Try again in about ${remaining} minute${remaining === 1 ? "" : "s"}.`
    );
  }
}

function resolveRidePaymentStatus(membershipType, amountCents) {
  if (!amountCents || amountCents <= 0) {
    return {
      paymentStatus: "included",
      paymentMethod: "included",
    };
  }
  const normalizedPlan = normalizeMembershipPlan(membershipType || "basic");
  if (normalizedPlan === "basic") {
    return {
      paymentStatus: "pending",
      paymentMethod: "stripe",
    };
  }
  return {
    paymentStatus: "preauthorized",
    paymentMethod: "stripe",
  };
}

function serializeRideForClient(rideDoc) {
  if (!rideDoc) {
    return null;
  }
  const clientRide = {
    ...rideDoc,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
  if (rideDoc.pinGeneratedAt) {
    clientRide.pinGeneratedAt = Date.now();
  }
  return clientRide;
}

exports.saveUserProfile = functions.https.onCall(async (data, context) => {
  const uid = requireAuth(context);
  const payload = typeof data === "object" && data !== null ? data : {};
  const userRef = db.collection("users").doc(uid);
  const snap = await userRef.get();
  const existingRaw = snap.exists ? snap.data() || {} : {};
  const existing = await maybeDowngradeExpiredMembership(userRef, existingRaw);

  const profileUpdates = {
    fullName: sanitizeProfileString(payload.fullName, 80) || existing.fullName || "",
    gender: normalizePoolGender(payload.gender) || existing.gender || null,
    phone: sanitizePhoneNumber(payload.phone || payload.phoneNumber) || existing.phone || "",
    street: sanitizeProfileString(payload.street || payload.streetAddress, 140) || existing.street || "",
    city: sanitizeProfileString(payload.city, 80) || existing.city || "",
    state: sanitizeProfileString(payload.state, 40) || existing.state || "",
    zip: sanitizeProfileString(payload.zip, 20) || existing.zip || "",
    profilePicUrl: payload.profilePicUrl || existing.profilePicUrl || null,
    licensePicUrl: payload.licensePicUrl || existing.licensePicUrl || null,
    studentIdPicUrl: payload.studentIdPicUrl || existing.studentIdPicUrl || null,
    isStudent: normalizeBooleanInput(
      payload.isStudent !== undefined ? payload.isStudent : existing.isStudent
    ),
    updatedAt: FieldValue.serverTimestamp(),
  };

  const membershipType = normalizeMembershipPlan(existing.membershipType || "basic");
  const membershipStatus = existing.membershipStatus || "none";

  const userEmail =
    sanitizeProfileString(payload.email, 120) ||
    existing.email ||
    context?.auth?.token?.email ||
    null;

  const docPayload = {
    ...profileUpdates,
    email: userEmail,
    membershipType,
    membershipStatus,
  };

  if (!snap.exists) {
    docPayload.createdAt = FieldValue.serverTimestamp();
    docPayload.userId = uid;
  }

  if (payload.membershipTermsAccepted === true && !existing.membershipTermsAccepted) {
    docPayload.membershipTermsAccepted = true;
    docPayload.membershipTermsAcceptedAt = FieldValue.serverTimestamp();
    docPayload.membershipTermsVersion =
      sanitizeProfileString(payload.membershipTermsVersion, 20) || "v1";
  }

  await userRef.set(docPayload, { merge: true });

  return {
    status: "saved",
    profile: {
      ...existing,
      ...docPayload,
      updatedAt: Date.now(),
    },
  };
});

exports.createRideRequest = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onCall(async (data, context) => {
    let uid = null;
    let rideRef = null;
    let rideDoc = null;

    try {
      uid = requireAuth(context);
      const rideInput = extractRideInput(data?.ride || data);
      if (!rideInput) {
        throw new functions.https.HttpsError(
          "invalid-argument",
          "Ride payload is required."
        );
      }
      const pickupLocation =
        coerceLatLng(rideInput.pickupLocation) ||
        coerceLatLng(rideInput.fromLocation) ||
        null;
      const dropoffLocation =
        coerceLatLng(rideInput.dropoffLocation) ||
        coerceLatLng(rideInput.toLocation) ||
        null;
      if (!pickupLocation || !dropoffLocation) {
        throw new functions.https.HttpsError(
          "invalid-argument",
          "Pickup and dropoff coordinates are required."
        );
      }
      const estimatedMinutes = Number(rideInput.estimatedDurationMinutes);
      if (!Number.isFinite(estimatedMinutes) || estimatedMinutes <= 0) {
        throw new functions.https.HttpsError(
          "invalid-argument",
          "Estimated ride duration is required."
        );
      }

      const { profile, userRef } = await loadUserProfileOrThrow(uid);
      const membershipType = normalizeMembershipPlan(profile.membershipType || "basic");
      const membershipStatus = profile.membershipStatus || "none";
      await enforceRideCooldown(uid, membershipType);

      const extraStops = normalizeExtraStopsInput(rideInput.extraStops);
      const numRiders = Math.max(1, Math.min(6, Number(rideInput.numRiders) || 1));
      const maxRiders = Math.max(numRiders, Math.min(6, Number(rideInput.maxRiders) || numRiders));
      const isGroupRide = Boolean(rideInput.isGroupRide) || maxRiders > 1;
      const poolType = rideInput.poolType === "uofa" ? "uofa" : null;
      const normalizedStatus = normalizeRideStatus(rideInput.status, poolType);

      const fareBreakdown = computeFareForMembership(
        membershipType,
        estimatedMinutes,
        Boolean(rideInput.inHomeZone)
      );
      const totalCents = Math.max(
        0,
        Math.round(
          Number.isFinite(fareBreakdown.total) ? fareBreakdown.total * 100 : rideInput.totalCents
        )
      );

      const chargeContext = calculateRideChargeContext({
        membershipType,
        membershipStatus,
        pickupLocation,
        dropoffLocation,
        totalCents,
      });
      const amountCents = Math.max(0, Math.round(chargeContext.amountCents || 0));
      const paymentMeta = resolveRidePaymentStatus(membershipType, amountCents);

      rideDoc = buildRidePayload(
        {
          ...rideInput,
          extraStops,
          pickupLocation,
          dropoffLocation,
          fare: fareBreakdown,
          estimatedDurationMinutes: estimatedMinutes,
          numRiders,
          maxRiders,
        },
        {
          uid,
          membershipType,
          membershipStatus,
          pickupLocation,
          dropoffLocation,
          amountCents,
          totalCents,
          chargeContext,
        }
      );

      rideDoc.userId = uid;
      rideDoc.startedByUserId = uid;
      rideDoc.status = normalizedStatus;
      rideDoc.paymentStatus = paymentMeta.paymentStatus;
      rideDoc.paymentMethod = paymentMeta.paymentMethod;
      rideDoc.stripeAmountCents = amountCents;
      rideDoc.stripeAmount = amountCents / 100;
      rideDoc.stripeCurrency = "usd";
      rideDoc.totalCents = totalCents;
      rideDoc.fare = fareBreakdown;
      rideDoc.extraStops = extraStops.length ? extraStops : [];
      rideDoc.maxRiders = maxRiders;
      rideDoc.currentRiderCount = Math.min(numRiders, maxRiders);
      rideDoc.isGroupRide = isGroupRide;
      rideDoc.poolType = poolType;
      rideDoc.gender =
        normalizePoolGender(profile.gender) ||
        rideDoc.gender ||
        null;
      rideDoc.isStudent = Boolean(profile.isStudent);
      rideDoc.uofaVerified = Boolean(profile.uofaVerified);
      rideDoc.uofaPoolEligible =
        membershipType === "uofa_unlimited" &&
        Boolean(profile.isStudent) &&
        Boolean(profile.uofaVerified) &&
        Boolean(rideDoc.gender) &&
        Boolean(chargeContext.pickupInside && chargeContext.dropoffInside);
      const riderName =
        sanitizeProfileString(
          profile.fullName ||
            profile.name ||
            profile.displayName ||
            (profile.email ? profile.email.split("@")[0] : ""),
          80
        ) || "Rider";
      const riderPhone =
        sanitizePhoneNumber(profile.phone || profile.phoneNumber || "") || "";
      const riderPhotoUrl =
        sanitizeProfileString(profile.profilePicUrl || profile.photoURL || "", 500) || null;
      rideDoc.riderName = riderName;
      rideDoc.riderPhone = riderPhone;
      rideDoc.riderProfilePicUrl = riderPhotoUrl;
      rideDoc.riderPhotoUrl = riderPhotoUrl;
      rideDoc.riderAvatarUrl = riderPhotoUrl;
      rideDoc.riderImageUrl = riderPhotoUrl;
      rideDoc.membershipStatus = membershipStatus;
      rideDoc.createdAt = FieldValue.serverTimestamp();
      rideDoc.updatedAt = FieldValue.serverTimestamp();

      const pinUpdates = ensurePinUpdates(rideDoc);
      if (pinUpdates) {
        Object.assign(rideDoc, pinUpdates);
      }

      rideRef = db.collection("rideRequests").doc();
      if (rideDoc.isGroupRide && !rideDoc.groupId) {
        rideDoc.groupId = rideRef.id;
      }

      removeUndefinedFields(rideDoc);

      await rideRef.set(rideDoc);
      await userRef.set({ lastRideRequestedAt: FieldValue.serverTimestamp() }, { merge: true });

      functions.logger.info("[createRideRequest] ride created", {
        uid,
        rideId: rideRef.id,
        paymentStatus: rideDoc.paymentStatus,
        extraStopsCount: rideDoc.extraStops?.length || 0,
      });

      return {
        rideId: rideRef.id,
        paymentStatus: rideDoc.paymentStatus,
        amountCents,
        ride: serializeRideForClient(rideDoc),
      };
    } catch (error) {
      functions.logger.error("[createRideRequest] failed", {
        uid: uid || context?.auth?.uid || null,
        rideId: rideRef?.id || null,
        code: error?.code || null,
        message: error?.message || "Unknown error",
      });
      throw error;
    }
  });

exports.joinRideGroup = functions.https.onCall(async (data, context) => {
  const uid = requireAuth(context);
  const hostRideId = data?.hostRideId || data?.rideId;
  if (!hostRideId) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "Host ride ID is required."
    );
  }
  const { profile } = await loadUserProfileOrThrow(uid);
  const membershipType = normalizeMembershipPlan(profile.membershipType || "basic");
  const membershipStatus = profile.membershipStatus || "none";
  const riderGender = normalizePoolGender(profile.gender);
  if (!riderGender) {
    throw new functions.https.HttpsError(
      "failed-precondition",
      "Set your profile gender before joining pooled rides."
    );
  }
  await enforceRideCooldown(uid, membershipType);

  const hostRef = db.collection("rideRequests").doc(hostRideId);
  const rideRef = db.collection("rideRequests").doc();
  let createdRideId = null;

  await db.runTransaction(async (tx) => {
    const hostSnap = await tx.get(hostRef);
    if (!hostSnap.exists) {
      throw new functions.https.HttpsError(
        "not-found",
        "This group ride is no longer available."
      );
    }
    const host = hostSnap.data() || {};
    if (!isRideAvailableForPooling(host)) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "This ride is no longer open for pooling."
      );
    }
    const hostGender = normalizePoolGender(host.gender);
    if (!hostGender || hostGender !== riderGender) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "This group ride is restricted to riders with matching gender."
      );
    }

    const maxRiders = Math.max(1, host.maxRiders || 1);
    const currentCount = Math.max(1, host.currentRiderCount || 1);
    if (currentCount >= maxRiders) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "This group ride is already full."
      );
    }

    const pickupLocation = coerceLatLng(host.pickupLocation);
    const dropoffLocation = coerceLatLng(host.dropoffLocation);
    if (!pickupLocation || !dropoffLocation) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Ride locations are incomplete."
      );
    }

    const minutes = Number(host.estimatedDurationMinutes) || 0;
    const fareBreakdown = computeFareForMembership(
      membershipType,
      minutes,
      Boolean(host.inHomeZone)
    );
    const totalCents = Math.max(
      0,
      Math.round(
        Number.isFinite(fareBreakdown.total) ? fareBreakdown.total * 100 : host.totalCents || 0
      )
    );
    const chargeContext = calculateRideChargeContext({
      membershipType,
      membershipStatus,
      pickupLocation,
      dropoffLocation,
      totalCents,
    });
    const amountCents = Math.max(0, Math.round(chargeContext.amountCents || 0));
    const paymentMeta = resolveRidePaymentStatus(membershipType, amountCents);

    const pickupCode =
      normalizeRideCode(host.pickupCode) ||
      normalizeRideCode(host.pickupPin) ||
      generateRideCode();
    const dropoffCode =
      normalizeRideCode(host.dropoffCode) ||
      normalizeRideCode(host.dropoffPin) ||
      generateRideCode();

    const joinRideDoc = {
      userId: uid,
      startedByUserId: host.startedByUserId || host.userId || uid,
      membershipType,
      membershipStatus,
      isStudent: Boolean(profile.isStudent),
      uofaVerified: Boolean(profile.uofaVerified),
      gender: riderGender,
      isGroupRide: true,
      groupId: host.groupId || hostRideId,
      maxRiders,
      currentRiderCount: 1,
      poolType: host.poolType || null,
      pickupLocation,
      dropoffLocation,
      fare: fareBreakdown,
      totalCents,
      stripeAmountCents: amountCents,
      stripeAmount: amountCents / 100,
      stripeCurrency: "usd",
      paymentStatus: paymentMeta.paymentStatus,
      paymentMethod: paymentMeta.paymentMethod,
      status: host.status || "pool_searching",
      pickupCode,
      pickupPin: pickupCode,
      dropoffCode,
      dropoffPin: dropoffCode,
      createdAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    };

    tx.set(rideRef, joinRideDoc);
    tx.update(hostRef, {
      currentRiderCount: Math.min(maxRiders, currentCount + 1),
      updatedAt: FieldValue.serverTimestamp(),
    });

    createdRideId = rideRef.id;
  });

  return { rideId: createdRideId };
});

exports.cancelRideRequest = functions.https.onCall(async (data, context) => {
  const uid = requireAuth(context);
  const rideId = data?.rideId;
  if (!rideId) {
    throw new functions.https.HttpsError("invalid-argument", "Ride ID is required.");
  }
  const rideRef = db.collection("rideRequests").doc(rideId);
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(rideRef);
    if (!snap.exists) {
      throw new functions.https.HttpsError("not-found", "Ride not found.");
    }
    const ride = snap.data() || {};
    if (ride.userId !== uid) {
      throw new functions.https.HttpsError(
        "permission-denied",
        "You can only cancel your own rides."
      );
    }
    if (!RIDER_CANCELABLE_STATUSES.has(ride.status)) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "This ride can no longer be canceled."
      );
    }
    tx.update(rideRef, {
      status: "canceled_by_rider",
      paymentStatus: ride.paymentStatus === "paid" ? ride.paymentStatus : "canceled",
      canceledAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    });
  });
  return { status: "canceled" };
});

exports.submitRideRating = functions.https.onCall(async (data, context) => {
  const uid = requireAuth(context);
  const rideId = data?.rideId;
  const rating = Number(data?.rating);
  const feedback =
    typeof data?.feedback === "string" ? data.feedback.trim().slice(0, 500) : "";
  if (!rideId) {
    throw new functions.https.HttpsError("invalid-argument", "Ride ID is required.");
  }
  if (!Number.isFinite(rating) || rating < RIDER_RATING_MIN || rating > RIDER_RATING_MAX) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "Rating must be between 1 and 5."
    );
  }
  const rideRef = db.collection("rideRequests").doc(rideId);
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(rideRef);
    if (!snap.exists) {
      throw new functions.https.HttpsError("not-found", "Ride not found.");
    }
    const ride = snap.data() || {};
    if (ride.userId !== uid) {
      throw new functions.https.HttpsError(
        "permission-denied",
        "You can only rate your own rides."
      );
    }
    tx.update(rideRef, {
      rating,
      riderRating: rating,
      riderFeedback: feedback || FieldValue.delete(),
      riderRated: true,
      riderRatedAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    });
  });
  return { status: "recorded" };
});

exports.confirmRidePaymentIntent = functions
  .runWith({ secrets: STRIPE_SECRET_PARAMS })
  .https.onCall(async (data, context) => {
    const uid = requireAuth(context);
    const rideId = data?.rideId;
    const paymentIntentId = data?.paymentIntentId;
    if (!rideId || !paymentIntentId) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Ride ID and payment intent ID are required."
      );
    }
    const rideRef = db.collection("rideRequests").doc(rideId);
    const snap = await rideRef.get();
    if (!snap.exists) {
      throw new functions.https.HttpsError("not-found", "Ride not found.");
    }
    const ride = snap.data() || {};
    if (ride.userId !== uid) {
      throw new functions.https.HttpsError(
        "permission-denied",
        "You can only update payments for your own rides."
      );
    }
    const stripeClient = getStripeClient();
    const intent = await stripeClient.paymentIntents.retrieve(paymentIntentId);
    const metadataRideId = intent.metadata?.rideId;
    const metadataUserId = intent.metadata?.userId;
    if (!metadataRideId) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Payment intent is missing ride metadata."
      );
    }
    if (metadataRideId !== rideId) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Payment intent does not match this ride."
      );
    }
    if (!metadataUserId) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Payment intent is missing user metadata."
      );
    }
    if (metadataUserId !== uid) {
      throw new functions.https.HttpsError(
        "permission-denied",
        "Payment intent is associated with another user."
      );
    }
    if (intent.status !== "succeeded") {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Stripe has not confirmed this payment yet."
      );
    }

    const amountReceived = Number(intent.amount_received || intent.amount || 0);
    const baseAmountCents = Number(intent.metadata?.base_amount_cents) || amountReceived;
    const tipAmountCents = Number(intent.metadata?.tip_amount_cents) || 0;

    await rideRef.update({
      paymentStatus: "paid",
      stripePaymentIntentId: intent.id,
      stripeAmountCents: amountReceived,
      stripeAmount: amountReceived / 100,
      stripeCurrency: intent.currency || "usd",
      fareBaseAmountCents: baseAmountCents,
      tipAmountCents,
      tipAmount: tipAmountCents / 100,
      tipCurrency: intent.currency || "usd",
      updatedAt: FieldValue.serverTimestamp(),
    });

    return { status: "updated" };
  });

exports.getDriverAvailabilityStats = functions.https.onCall(async (_data, context) => {
  requireAuth(context);

  try {
    const driversSnap = await db
      .collection("drivers")
      .where("isOnline", "==", true)
      .select("isOnline")
      .get();

    const onlineDriverIds = [];
    driversSnap.forEach((docSnap) => {
      onlineDriverIds.push(docSnap.id);
    });

    if (!onlineDriverIds.length) {
      return {
        onlineDriverIds: [],
        busyDriverIds: [],
        onlineCount: 0,
        busyCount: 0,
        availableCount: 0,
        hasOnlineDrivers: false,
        hasAvailableDrivers: false,
        generatedAt: Date.now(),
      };
    }

    const busyDriverSnap = await db
      .collection("rideRequests")
      .where("status", "in", DRIVER_BUSY_STATUSES)
      .select("driverId", "assignedDriverId")
      .get();

    const onlineDriverIdSet = new Set(onlineDriverIds);
    const busyDriverIdSet = new Set();
    busyDriverSnap.forEach((docSnap) => {
      const payload = docSnap.data() || {};
      const driverId = payload.driverId || payload.assignedDriverId || null;
      if (driverId && onlineDriverIdSet.has(driverId)) {
        busyDriverIdSet.add(driverId);
      }
    });

    const onlineCount = onlineDriverIds.length;
    const busyDriverIds = Array.from(busyDriverIdSet);
    const busyCount = busyDriverIds.length;
    const availableCount = Math.max(0, onlineCount - busyCount);

    return {
      onlineDriverIds,
      busyDriverIds,
      onlineCount,
      busyCount,
      availableCount,
      hasOnlineDrivers: onlineCount > 0,
      hasAvailableDrivers: availableCount > 0,
      generatedAt: Date.now(),
    };
  } catch (err) {
    console.error("[getDriverAvailabilityStats] Error:", err);
    throw new functions.https.HttpsError(
      "internal",
      "Unable to load driver availability right now."
    );
  }
});

exports.__testables = {
  computeFareForMembership,
  calculateRideChargeContext,
  resolveMembershipPlan,
  computeMembershipExpirationTimestamp,
  normalizePoolGender,
  gendersCompatible,
  extractUofaRideDetails,
  isUofaEligibleRide,
  isRideAvailableForPooling,
};

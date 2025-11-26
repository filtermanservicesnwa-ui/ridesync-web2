// functions/index.js

const functions = require("firebase-functions/v1");
const admin = require("firebase-admin");
const Stripe = require("stripe");

const runtimeConfig = (() => {
  try {
    return functions.config() || {};
  } catch (err) {
    return {};
  }
})();

function resolveStripeSettings() {
  const stripeConfig = runtimeConfig?.stripe || {};
  const env = process.env || {};
  return {
    secretKey:
      env.STRIPE_SECRET_KEY ||
      stripeConfig.secret_key ||
      stripeConfig.secretKey ||
      null,
    uofaPriceId:
      env.STRIPE_UOFA_PRICE_ID ||
      stripeConfig.uofa_price_id ||
      stripeConfig.uofaPriceId ||
      null,
    nwaPriceId:
      env.STRIPE_NWA_PRICE_ID ||
      stripeConfig.nwa_price_id ||
      stripeConfig.nwaPriceId ||
      null,
  };
}

const stripeSettings = resolveStripeSettings();
let stripe = null;
let uofaPriceId = stripeSettings.uofaPriceId || null;
let nwaPriceId = stripeSettings.nwaPriceId || null;

if (stripeSettings.secretKey) {
  stripe = Stripe(stripeSettings.secretKey);
} else {
  console.warn(
    "[RideSync][Stripe] Missing Stripe secret key. Set stripe.secret_key runtime config or STRIPE_SECRET_KEY env to enable billing."
  );
}
if (!uofaPriceId) {
  console.warn(
    "[RideSync][Stripe] Missing U of A Stripe price ID. Set stripe.uofa_price_id or STRIPE_UOFA_PRICE_ID."
  );
}
if (!nwaPriceId) {
  console.warn(
    "[RideSync][Stripe] Missing NWA Stripe price ID. Set stripe.nwa_price_id or STRIPE_NWA_PRICE_ID."
  );
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
  basic: { amountCents: 0, currency: "usd", label: "Basic (pay per ride)" },
  uofa_unlimited: {
    amountCents: 8000,
    currency: "usd",
    label: "U of A Unlimited ($80/mo)",
  },
  nwa_unlimited: {
    amountCents: 12000,
    currency: "usd",
    label: "NWA Unlimited ($120/mo)",
  },
};

const MEMBERSHIP_PLAN_ALIASES = {
  basic: new Set(["basic", "basic (pay per ride)", "basic plan", "plan: basic"]),
};

const DEFAULT_FARE_CONSTANTS = {
  BASIC_RATE_PER_MIN: 0.6,
  BASIC_PLATFORM_FEE: 0.5,
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

function getStripeClient() {
  if (!stripe) {
    throw new functions.https.HttpsError(
      "failed-precondition",
      "Stripe secret key is not configured."
    );
  }
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

  return {
    ...defaults,
    amountCents,
    plan,
  };
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

async function getOrCreateStripeCustomer(uid) {
  const userRef = db.collection("users").doc(uid);
  const snap = await userRef.get();
  const profile = snap.exists ? snap.data() : null;
  if (profile?.stripeCustomerId) {
    return {
      customerId: profile.stripeCustomerId,
      profileRef: userRef,
      profile,
    };
  }

  const stripe = getStripeClient();
  const customer = await stripe.customers.create({
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

  return {
    customerId: customer.id,
    profileRef: userRef,
    profile,
  };
}

function cloneData(input) {
  return JSON.parse(JSON.stringify(input ?? {}));
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

exports.createMembershipPaymentIntent = functions.https.onCall(
  async (data, context) => {
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

    const stripe = getStripeClient();
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
    };
  }
);

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

  if (planKey === "basic") {
    await userRef.set(
      {
        membershipType: "basic",
        membershipStatus: "none",
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
        membershipExpiresAt: null,
        membershipApprovalRequired: needsApproval || FieldValue.delete(),
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

  const stripe = getStripeClient();
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
      membershipPaidAmountCents: pending.amountCents,
      membershipPaidCurrency: pending.currency,
      membershipStripePaymentIntentId: paymentIntentId,
      membershipApprovalRequired: needsApproval || FieldValue.delete(),
    },
    { merge: true }
  );

  await pendingRef.delete();

  return { status: "updated" };
}

exports.applyMembershipPlan = functions.https.onCall(async (data, context) => {
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

exports.applyMembershipPlanHttp = functions.https.onRequest(
  async (req, res) => {
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
  }
);

// === RIDE SYNC STRIPE: START createMembershipSubscriptionIntent ===
exports.createMembershipSubscriptionIntent = functions.https.onCall(
  async (data, context) => {
    const uid = requireAuth(context);
    const planId = normalizeMembershipPlan(data?.planId || data?.plan);
    if (!["uofa_unlimited", "nwa_unlimited"].includes(planId)) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Unsupported membership plan."
      );
    }
    const priceId = planId === "uofa_unlimited" ? uofaPriceId : nwaPriceId;
    if (!priceId) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Stripe price ID is not configured for this plan."
      );
    }

    const stripeClient = getStripeClient();
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

    const clientSecret =
      subscription?.latest_invoice?.payment_intent?.client_secret;
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
    };
  }
);
// === RIDE SYNC STRIPE: END createMembershipSubscriptionIntent ===

// === RIDE SYNC STRIPE: START finalizeMembershipSubscription ===
exports.finalizeMembershipSubscription = functions.https.onCall(
  async (data, context) => {
    const uid = requireAuth(context);
    const subscriptionId = data?.subscriptionId;
    if (!subscriptionId) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Stripe subscription ID is required."
      );
    }

    const stripeClient = getStripeClient();
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
  }
);
// === RIDE SYNC STRIPE: END finalizeMembershipSubscription ===

// === RIDE SYNC STRIPE: START createRidePaymentIntent ===
exports.createRidePaymentIntent = functions.https.onCall(
  async (data, context) => {
    const uid = requireAuth(context);
    const rideInput = data?.ride;
    if (!rideInput) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Ride payload is required."
      );
    }

    const totalCentsInput = Number(rideInput.totalCents);
    if (!Number.isFinite(totalCentsInput)) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Estimated fare (totalCents) is required."
      );
    }
    const totalCents = Math.max(0, Math.round(totalCentsInput));

    const pickupLocation =
      rideInput.pickupLocation ||
      rideInput.fromLocation ||
      rideInput.currentLocation;
    const dropoffLocation =
      rideInput.dropoffLocation ||
      rideInput.toLocation ||
      rideInput.destinationLocation;
    if (!hasValidLocation(pickupLocation) || !hasValidLocation(dropoffLocation)) {
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

    const userRef = db.collection("users").doc(uid);
    const userSnap = await userRef.get();
    const profile = userSnap.exists ? userSnap.data() : {};
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

    if (chargeContext.amountCents <= 0) {
      const rideRef = db.collection("rideRequests").doc();
      const rideData = {
        ...sanitizedPayload,
        paymentMethod: "included",
        paymentStatus: "included",
        createdAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      };
      if (rideData.isGroupRide && !rideData.groupId) {
        rideData.groupId = rideRef.id;
      }
      await rideRef.set(rideData);
      return {
        skipPayment: true,
        rideId: rideRef.id,
        status: rideData.status,
        geofenceContext: chargeContext,
      };
    }

    const pendingRef = db.collection("pendingRidePayments").doc();

    const stripeClient = getStripeClient();
    const { customerId } = await getOrCreateStripeCustomer(uid);

    const paymentIntent = await stripeClient.paymentIntents.create({
      amount: chargeContext.amountCents,
      currency: "usd",
      customer: customerId,
      metadata: {
        firebaseUid: uid,
        pendingRideId: pendingRef.id,
        purpose: "ride",
      },
      automatic_payment_methods: { enabled: true },
    });

    await pendingRef.set({
      userId: uid,
      pendingId: pendingRef.id,
      ridePayload: sanitizedPayload,
      amountCents: chargeContext.amountCents,
      totalCents,
      geofenceContext: chargeContext,
      currency: "usd",
      stripePaymentIntentId: paymentIntent.id,
      createdAt: FieldValue.serverTimestamp(),
    });

    return {
      clientSecret: paymentIntent.client_secret,
      paymentIntentId: paymentIntent.id,
      pendingId: pendingRef.id,
      amountCents: chargeContext.amountCents,
      currency: "usd",
      geofenceContext: chargeContext,
    };
  }
);
// === RIDE SYNC STRIPE: END createRidePaymentIntent ===

exports.finalizeRidePayment = functions.https.onCall(
  async (data, context) => {
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

    const stripe = getStripeClient();
    const intent = await stripe.paymentIntents.retrieve(paymentIntentId);
    if (intent.status !== "succeeded") {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "Ride payment has not completed."
      );
    }

    const rideData = cloneData(pending.ridePayload || {});
    rideData.userId = uid;
    rideData.startedByUserId = rideData.startedByUserId || uid;
    rideData.status = normalizeRideStatus(
      rideData.status,
      rideData.poolType
    );
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
      });
    });

    return { rideId: rideRef.id };
  }
);

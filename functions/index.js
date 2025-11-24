// functions/index.js

// IMPORTANT: use v1 compat entrypoint so functions.firestore.document(...) exists
const functions = require("firebase-functions/v1");
const admin = require("firebase-admin");
const Stripe = require("stripe");

if (!admin.apps.length) {
  admin.initializeApp();
}

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

const FARE_CONSTANTS = {
  BASIC_RATE_PER_MIN: 0.6,
  BASIC_PLATFORM_FEE: 0.5,
  UNLIMITED_OUT_RATE: 0.35,
  PROCESSING_FEE_RATE: 0.04,
};

const RIDE_STATUS_ALLOWLIST = new Set([
  "pending_driver",
  "pool_searching",
  "pooled_pending_driver",
  "pending",
]);

const runtimeConfig = (() => {
  try {
    return functions.config() || {};
  } catch (err) {
    return {};
  }
})();

const stripeSecretKey =
  process.env.STRIPE_SECRET_KEY ||
  runtimeConfig?.stripe?.secret ||
  runtimeConfig?.stripe?.sk ||
  null;

let stripeClient = null;

function normalizeMembershipPlan(value = "") {
  return String(value || "").toLowerCase().trim();
}

function getStripeClient() {
  if (!stripeSecretKey) {
    throw new functions.https.HttpsError(
      "failed-precondition",
      "Stripe secret key is not configured."
    );
  }
  if (!stripeClient) {
    stripeClient = new Stripe(stripeSecretKey, {
      apiVersion: "2024-06-20",
    });
  }
  return stripeClient;
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
  const plan = String(planRaw || "basic").toLowerCase();
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
    UNLIMITED_OUT_RATE,
    PROCESSING_FEE_RATE,
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
    processingFee = rideSubtotal * PROCESSING_FEE_RATE;
    total = rideSubtotal + processingFee;
    membershipLabel =
      plan === "uofa_unlimited"
        ? "U of A Unlimited – out of Fayetteville (extra per-minute)"
        : "NWA Unlimited – out of zone (extra per-minute)";
  } else {
    rideSubtotal = clampedMinutes * BASIC_RATE_PER_MIN + BASIC_PLATFORM_FEE;
    processingFee = rideSubtotal * PROCESSING_FEE_RATE;
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
    hasValidLocation(details.fromLocation)
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

exports.ensureRidePins = functions.firestore
  .document("rideRequests/{rideId}")
  .onCreate(async (snap, context) => {
    try {
      const ride = snap.data() || {};
      const updates = ensurePinUpdates(ride);
      if (!updates) {
        return null;
      }
      await snap.ref.update(updates);
      console.log(
        `[ensureRidePins] Added ride codes for ${context.params.rideId}.`
      );
      return null;
    } catch (err) {
      console.error("[ensureRidePins] Error:", err);
      return null;
    }
  });

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

exports.applyMembershipPlan = functions.https.onCall(async (data, context) => {
  const uid = requireAuth(context);
  const planKey = normalizeMembershipPlan(data?.plan);
  const planConfig = resolveMembershipPlan(planKey);
  if (!planConfig) {
    throw new functions.https.HttpsError(
      "invalid-argument",
      "Unknown membership plan."
    );
  }

  const userRef = db.collection("users").doc(uid);
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
});

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

    const minutes = Number(rideInput.estimatedDurationMinutes);
    if (!Number.isFinite(minutes) || minutes <= 0) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Estimated minutes missing for ride payment."
      );
    }
    if (
      !rideInput.toDestination ||
      typeof rideInput.toDestination !== "string"
    ) {
      throw new functions.https.HttpsError(
        "invalid-argument",
        "Destination is required."
      );
    }

    const plan = normalizeMembershipPlan(
      rideInput.membershipType ||
        rideInput.membership ||
        rideInput.plan ||
        "basic"
    );
    const fare = computeFareForMembership(
      plan,
      minutes,
      !!rideInput.inHomeZone
    );
    const amountCents = Math.round(fare.total * 100);
    if (amountCents <= 0) {
      throw new functions.https.HttpsError(
        "failed-precondition",
        "This ride does not require a Stripe payment."
      );
    }

    const ridePayloadRaw = { ...rideInput };
    delete ridePayloadRaw.createdAt;
    delete ridePayloadRaw.updatedAt;

    const sanitizedPayload = cloneData(ridePayloadRaw);
    sanitizedPayload.userId = uid;
    sanitizedPayload.startedByUserId =
      sanitizedPayload.startedByUserId || uid;
    sanitizedPayload.membershipType = plan;
    sanitizedPayload.membershipStatus =
      sanitizedPayload.membershipStatus || "active";
    sanitizedPayload.inHomeZone = !!rideInput.inHomeZone;
    sanitizedPayload.isGroupRide = !!rideInput.isGroupRide;
    sanitizedPayload.maxRiders = Math.max(
      1,
      Math.min(6, sanitizedPayload.maxRiders || 1)
    );
    sanitizedPayload.currentRiderCount = Math.min(
      sanitizedPayload.maxRiders,
      sanitizedPayload.currentRiderCount || 1
    );
    sanitizedPayload.poolType =
      sanitizedPayload.poolType === "uofa" ? "uofa" : null;
    sanitizedPayload.status = normalizeRideStatus(
      sanitizedPayload.status,
      sanitizedPayload.poolType
    );
    sanitizedPayload.fare = fare;
    sanitizedPayload.estimatedDurationMinutes = minutes;
    sanitizedPayload.stripeAmount = fare.total;
    sanitizedPayload.stripeAmountCents = amountCents;
    sanitizedPayload.stripeCurrency = "usd";
    sanitizedPayload.paymentMethod = "stripe";
    sanitizedPayload.paymentStatus = "paid";
    sanitizedPayload.createdAt = null;
    sanitizedPayload.updatedAt = null;
    sanitizedPayload.toDestination = sanitizeDestinationLabel(
      rideInput.toDestination
    );

    const pendingRef = db.collection("pendingRidePayments").doc();

    const stripe = getStripeClient();
    const { customerId } = await getOrCreateStripeCustomer(uid);

    const paymentIntent = await stripe.paymentIntents.create({
      amount: amountCents,
      currency: "usd",
      customer: customerId,
      metadata: {
        firebaseUid: uid,
        pendingRideId: pendingRef.id,
        destination: sanitizedPayload.toDestination,
        purpose: "ride",
      },
      automatic_payment_methods: { enabled: true },
    });

    await pendingRef.set({
      userId: uid,
      pendingId: pendingRef.id,
      ridePayload: sanitizedPayload,
      amountCents,
      currency: "usd",
      stripePaymentIntentId: paymentIntent.id,
      createdAt: FieldValue.serverTimestamp(),
    });

    return {
      clientSecret: paymentIntent.client_secret,
      paymentIntentId: paymentIntent.id,
      pendingId: pendingRef.id,
      amountCents,
      currency: "usd",
    };
  }
);

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

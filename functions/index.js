// functions/index.js

// IMPORTANT: use v1 compat entrypoint so functions.firestore.document(...) exists
const functions = require("firebase-functions/v1");
const admin = require("firebase-admin");

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

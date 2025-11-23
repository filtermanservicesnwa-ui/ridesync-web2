// functions/index.js

// IMPORTANT: use v1 compat entrypoint so functions.firestore.document(...) exists
const functions = require("firebase-functions/v1");
const admin = require("firebase-admin");

if (!admin.apps.length) {
  admin.initializeApp();
}

const db = admin.firestore();

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
        .limit(10)
        .get();

      if (driversSnap.empty) {
        console.log("[notifyDriverOnNewRide] No online drivers with FCM tokens.");
        return null;
      }

      const tokens = [];
      driversSnap.forEach((docSnap) => {
        const d = docSnap.data();
        if (d.fcmToken) {
          tokens.push(d.fcmToken);
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
      return null;
    } catch (err) {
      console.error("[notifyDriverOnNewRide] Error:", err);
      return null;
    }
  });


exports.uofaAutoPool = functions.firestore
  .document("rideRequests/{rideId}")
  .onCreate(async (snap, context) => {
    const newRide = snap.data();
    const newRideId = context.params.rideId;

    try {
      // 1) Basic eligibility: must be U of A unlimited & verified & solo rider
      const membershipType =
        newRide.membershipType ||
        newRide.membership ||
        "";

      const isUofaUnlimited = membershipType === "uofa_unlimited";

      const isVerified =
        !!newRide.uofaVerified ||
        !!newRide.isUofaVerified ||
        false;

      const numRiders =
        newRide.numRiders ||
        newRide.riderCount ||
        1;

      if (!isUofaUnlimited || !isVerified || numRiders !== 1) {
        console.log(
          `[uofaAutoPool] Ride ${newRideId} not eligible for U of A pooling.`
        );
        return null;
      }

      // Optional: basic "Fayetteville only" guard if you store city
      const pickupCity =
        newRide.pickupCity ||
        newRide.city ||
        "";
      if (
        pickupCity &&
        pickupCity.toLowerCase() !== "fayetteville"
      ) {
        console.log(
          `[uofaAutoPool] Ride ${newRideId} not in Fayetteville (pickupCity=${pickupCity}).`
        );
        return null;
      }

      // 2) Need pickup coords to measure distance
      const fromLoc = newRide.fromLocation || newRide.pickupLocation;
      if (
        !fromLoc ||
        typeof fromLoc.lat !== "number" ||
        typeof fromLoc.lng !== "number"
      ) {
        console.log(
          `[uofaAutoPool] Ride ${newRideId} missing fromLocation lat/lng.`
        );
        return null;
      }

      // 3) Query other U of A rides with pending-like status
      const candidatesSnap = await db
        .collection("rideRequests")
        .where("membershipType", "==", "uofa_unlimited")
        .where("status", "in", [
          "pending_driver",
          "pool_searching",
          "pooled_pending_driver",
        ])
        .orderBy("createdAt", "desc")
        .limit(20)
        .get();

      let bestMatch = null;
      let bestDistance = Infinity;

      candidatesSnap.forEach((docSnap) => {
        const cid = docSnap.id;
        if (cid === newRideId) return;

        const data = docSnap.data();

        // Already has driver? skip
        if (data.driverId) return;

        // Already grouped with someone else? skip (for now)
        if (data.groupId) return;

        const cNumRiders =
          data.numRiders ||
          data.riderCount ||
          1;
        if (cNumRiders !== 1) return;

        const cIsVerified =
          !!data.uofaVerified ||
          !!data.isUofaVerified ||
          false;
        if (!cIsVerified) return;

        const cFromLoc = data.fromLocation || data.pickupLocation;
        if (
          !cFromLoc ||
          typeof cFromLoc.lat !== "number" ||
          typeof cFromLoc.lng !== "number"
        ) {
          return;
        }

        // (Optional) city guard
        const cCity =
          data.pickupCity ||
          data.city ||
          "";
        if (
          cCity &&
          pickupCity &&
          cCity.toLowerCase() !== pickupCity.toLowerCase()
        ) {
          return;
        }

        const dist = distanceKm(
          { lat: fromLoc.lat, lng: fromLoc.lng },
          { lat: cFromLoc.lat, lng: cFromLoc.lng }
        );

        if (dist < 3 && dist < bestDistance) {
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

      // 4) Make them a group
      const groupId = bestMatch.id; // use earlier ride as group host

      const newRideRef = snap.ref;
      const matchRideRef = db.collection("rideRequests").doc(bestMatch.id);

      const batch = db.batch();

      // update new ride
      batch.update(newRideRef, {
        poolType: "uofa",
        isGroupRide: true,
        groupId: groupId,
        currentRiderCount: 2,
        maxRiders: 2,
        status: "pooled_pending_driver",
      });

      // update matched ride
      batch.update(matchRideRef, {
        poolType: "uofa",
        isGroupRide: true,
        groupId: groupId,
        currentRiderCount: 2,
        maxRiders: 2,
        status: "pooled_pending_driver",
      });

      await batch.commit();

      console.log(
        `[uofaAutoPool] Created group ${groupId} with rides ${newRideId} & ${bestMatch.id}`
      );

      return null;
    } catch (err) {
      console.error("[uofaAutoPool] Error:", err);
      return null;
    }
  });

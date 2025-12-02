process.env.NODE_ENV = "test";

jest.mock("firebase-admin", () => {
  const FieldValue = {
    serverTimestamp: jest.fn(() => ({ ".sv": "timestamp" })),
    delete: jest.fn(() => ({ ".sv": "delete" })),
  };

  class FakeTimestamp {
    constructor(millis) {
      this._millis = millis;
    }
    toMillis() {
      return this._millis;
    }
    toDate() {
      return new Date(this._millis);
    }
    static fromMillis(ms) {
      return new FakeTimestamp(ms);
    }
  }

  const firestoreInstance = {
    collection: jest.fn(() => ({
      doc: jest.fn(() => ({
        get: jest.fn(() => Promise.resolve({ exists: false })),
      })),
    })),
  };

  const firestoreFn = jest.fn(() => firestoreInstance);
  firestoreFn.FieldValue = FieldValue;
  firestoreFn.Timestamp = FakeTimestamp;

  return {
    initializeApp: jest.fn(),
    firestore: firestoreFn,
    auth: jest.fn(() => ({
      verifyIdToken: jest.fn(),
      getUser: jest.fn(),
    })),
    messaging: jest.fn(() => ({
      sendMulticast: jest.fn(),
    })),
  };
});

const {
  computeFareForMembership,
  calculateRideChargeContext,
  resolveMembershipPlan,
  computeMembershipExpirationTimestamp,
  normalizePoolGender,
  gendersCompatible,
  extractUofaRideDetails,
  isUofaEligibleRide,
  isRideAvailableForPooling,
  clampTipAmountCents,
  validateTipBounds,
  computeRideHoldAmountCents,
} = require("../index").__testables;

describe("fare calculation", () => {
  it("applies $1.50 platform fee to basic rides", () => {
    const result = computeFareForMembership("basic", 10, false);
    expect(result.rideSubtotal).toBeCloseTo(9, 5);
    expect(result.processingFee).toBeCloseTo(0.27, 5);
    expect(result.total).toBeCloseTo(9.27, 5);
  });

  it("treats unlimited in-zone rides as included", () => {
    const result = computeFareForMembership("uofa_unlimited", 15, true);
    expect(result.total).toBe(0);
    expect(result.membershipLabel).toBe("U OF A STUDENT — UNLIMITED");
  });
});

describe("checkout charge context", () => {
  const baseRide = {
    totalCents: 1500,
    pickupLocation: { lat: 36.063, lng: -94.171 },
    dropoffLocation: { lat: 36.064, lng: -94.17 },
  };

  it("waives fare for active unlimited members in zone", () => {
    const context = calculateRideChargeContext({
      ...baseRide,
      membershipType: "uofa_unlimited",
      membershipStatus: "active",
    });
    expect(context.amountCents).toBe(0);
    expect(context.pickupInside).toBe(true);
    expect(context.dropoffInside).toBe(true);
  });

  it("charges full fare when membership is inactive", () => {
    const context = calculateRideChargeContext({
      ...baseRide,
      membershipType: "uofa_unlimited",
      membershipStatus: "expired",
    });
    expect(context.amountCents).toBe(baseRide.totalCents);
  });
});

describe("membership helpers", () => {
  it("provides duration for unlimited plans", () => {
    const plan = resolveMembershipPlan("nwa_unlimited");
    expect(plan.durationDays).toBe(30);
  });

  it("creates expiration timestamps roughly 30 days ahead", () => {
    const now = Date.now();
    const expires = computeMembershipExpirationTimestamp(30);
    const deltaDays = (expires.toMillis() - now) / (24 * 60 * 60 * 1000);
    expect(deltaDays).toBeGreaterThan(29.5);
    expect(deltaDays).toBeLessThan(30.5);
  });
});

describe("pooling logic", () => {
  const sampleRide = {
    membershipType: "uofa_unlimited",
    uofaVerified: true,
    numRiders: 1,
    pickupLocation: { lat: 36.06, lng: -94.16 },
    gender: "Female",
    pickupCity: "Fayetteville",
  };

  it("marks single verified riders as eligible", () => {
    const details = extractUofaRideDetails(sampleRide);
    expect(isUofaEligibleRide(details)).toBe(true);
    expect(normalizePoolGender("FEMALE")).toBe("female");
  });

  it("rejects pooled rides once capacity or gender mismatches", () => {
    const details = extractUofaRideDetails({ ...sampleRide, numRiders: 2 });
    expect(isUofaEligibleRide(details)).toBe(false);
    expect(gendersCompatible("female", "male")).toBe(false);
    expect(
      isRideAvailableForPooling({
        status: "pending_driver",
        driverId: "driver-123",
      })
    ).toBe(false);
  });
});

describe("manual capture helpers", () => {
  it("clamps tip amounts within provided bounds", () => {
    expect(clampTipAmountCents(150, { min: 200, max: 1200 })).toBe(200);
    expect(clampTipAmountCents(1800, { min: 200, max: 1200 })).toBe(1200);
    expect(clampTipAmountCents(600, { min: 200, max: 1200 })).toBe(600);
  });

  it("validates tip selection with descriptive reasons", () => {
    const valid = validateTipBounds(500, { min: 200, max: 1200 });
    expect(valid.ok).toBe(true);
    expect(valid.value).toBe(500);

    const below = validateTipBounds(100, { min: 200, max: 1200 });
    expect(below.ok).toBe(false);
    expect(below.reason).toBe("below_min");

    const above = validateTipBounds(1800, { min: 200, max: 1200 });
    expect(above.ok).toBe(false);
    expect(above.reason).toBe("above_max");
  });

  it("computes the hold amount using fare plus max tip", () => {
    expect(computeRideHoldAmountCents(1500, 1200)).toBe(2700);
  });
});

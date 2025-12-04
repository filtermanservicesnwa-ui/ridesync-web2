const APP_CONFIG_URL = "/app-config.json";
const LOCAL_STORAGE_TOKEN_KEY = "ridesyncAdminToken";
const REFRESH_INTERVAL_MS = 60_000;
const FIREBASE_PROJECT_ID = "ride-sync-nwa";

const adminState = {
  token: null,
  endpoints: {},
  refreshTimer: null,
  activity: [],
  pending: [],
  stats: null,
};

const adminRoot = document.getElementById("adminRoot");
const adminLoginPanel = document.getElementById("adminLoginPanel");
const adminDashboard = document.getElementById("adminDashboard");
const loginButton = document.getElementById("adminLoginButton");
const loginError = document.getElementById("adminLoginError");
const screenNameInput = document.getElementById("adminScreenNameInput");
const passwordInput = document.getElementById("adminPasswordInput");
const ridersOnlineEl = document.getElementById("adminRidersOnlineCount");
const driversOnlineEl = document.getElementById("adminDriversOnlineCount");
const revenueTodayEl = document.getElementById("adminRevenueToday");
const ridesTodayEl = document.getElementById("adminRidesToday");
const activityFeedEl = document.getElementById("adminActivityFeed");
const pendingBodyEl = document.getElementById("adminUofaPendingBody");
const userSearchInput = document.getElementById("adminUserSearchInput");
const userSearchButton = document.getElementById("adminUserSearchButton");
const userDetailsEl = document.getElementById("adminUserDetails");

function formatCurrency(cents = 0) {
  const dollars = Math.max(0, Number(cents) || 0) / 100;
  return dollars.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
  });
}

function formatTimestamp(ms) {
  if (!ms || Number.isNaN(ms)) {
    return "--";
  }
  return new Date(ms).toLocaleString();
}

async function loadConfig() {
  const res = await fetch(APP_CONFIG_URL, { cache: "no-store" });
  if (!res.ok) {
    throw new Error("Unable to load app config");
  }
  return res.json();
}

function resolveEndpoint(overrides, key, fallbackBase, path) {
  if (overrides && typeof overrides[key] === "string") {
    return overrides[key];
  }
  if (fallbackBase) {
    return `${fallbackBase}/${path}`;
  }
  return "";
}

function buildAdminEndpoints(config = {}) {
  const functionsConfig = config.functions || {};
  const defaultBase = FIREBASE_PROJECT_ID
    ? `https://us-central1-${FIREBASE_PROJECT_ID}.cloudfunctions.net`
    : "";
  return {
    login: resolveEndpoint(functionsConfig, "adminLoginUrl", defaultBase, "adminLogin"),
    stats: resolveEndpoint(functionsConfig, "adminStatsUrl", defaultBase, "getAdminStats"),
    activity: resolveEndpoint(
      functionsConfig,
      "adminActivityUrl",
      defaultBase,
      "getAdminActivityFeed"
    ),
    pending: resolveEndpoint(
      functionsConfig,
      "adminPendingMembershipsUrl",
      defaultBase,
      "getPendingUofaMemberships"
    ),
    approve: resolveEndpoint(
      functionsConfig,
      "adminApproveMembershipUrl",
      defaultBase,
      "approveUofaMembership"
    ),
    reject: resolveEndpoint(
      functionsConfig,
      "adminRejectMembershipUrl",
      defaultBase,
      "rejectUofaMembership"
    ),
    search: resolveEndpoint(
      functionsConfig,
      "adminSearchUserUrl",
      defaultBase,
      "adminSearchUser"
    ),
    setMembership: resolveEndpoint(
      functionsConfig,
      "adminSetMembershipPlanUrl",
      defaultBase,
      "adminSetMembershipPlan"
    ),
  };
}

async function adminFetch(endpointKey, body = null) {
  const url = adminState.endpoints[endpointKey];
  if (!url) {
    throw new Error(`Missing endpoint for ${endpointKey}`);
  }
  const options = {
    method: body ? "POST" : "GET",
    headers: {
      "Content-Type": "application/json",
    },
  };
  if (adminState.token) {
    options.headers.Authorization = `Bearer ${adminState.token}`;
  }
  if (body) {
    options.body = JSON.stringify(body);
  }
  const res = await fetch(url, options);
  let data = null;
  try {
    data = await res.json();
  } catch (err) {
    data = null;
  }
  if (res.status === 401) {
    throw new Error("unauthorized");
  }
  if (!res.ok || (data && data.ok === false)) {
    const message = data?.error || res.statusText || "Request failed";
    throw new Error(message);
  }
  return data;
}

function showLoginPanel() {
  clearInterval(adminState.refreshTimer);
  adminState.refreshTimer = null;
  if (adminDashboard) {
    adminDashboard.style.display = "none";
  }
  if (adminLoginPanel) {
    adminLoginPanel.style.display = "block";
  }
  if (loginError) {
    loginError.textContent = "";
  }
}

function showDashboard() {
  if (adminLoginPanel) {
    adminLoginPanel.style.display = "none";
  }
  if (adminDashboard) {
    adminDashboard.style.display = "block";
  }
}

function persistToken(token) {
  adminState.token = token;
  if (token) {
    localStorage.setItem(LOCAL_STORAGE_TOKEN_KEY, token);
  } else {
    localStorage.removeItem(LOCAL_STORAGE_TOKEN_KEY);
  }
}

async function handleLogin() {
  const screenName = screenNameInput?.value?.trim();
  const password = passwordInput?.value || "";
  loginError.textContent = "";
  loginButton.disabled = true;
  try {
    const response = await adminFetch("login", { screenName, password });
    persistToken(response.token);
    showDashboard();
    await refreshAdminDashboard();
    scheduleAutoRefresh();
  } catch (err) {
    if (err.message === "unauthorized") {
      persistToken(null);
    }
    loginError.textContent = "Invalid admin username or password.";
  } finally {
    loginButton.disabled = false;
  }
}

function scheduleAutoRefresh() {
  clearInterval(adminState.refreshTimer);
  adminState.refreshTimer = setInterval(() => {
    refreshAdminDashboard().catch((err) => console.error(err));
  }, REFRESH_INTERVAL_MS);
}

async function refreshAdminDashboard() {
  if (!adminState.token) {
    return;
  }
  try {
    const [statsResponse, activityResponse, pendingResponse] = await Promise.all([
      adminFetch("stats", {}),
      adminFetch("activity", {}),
      adminFetch("pending", {}),
    ]);
    adminState.stats = statsResponse?.stats || null;
    adminState.activity = activityResponse?.feed || [];
    adminState.pending = pendingResponse?.pending || [];
    renderStats();
    renderActivityFeed();
    renderPendingTable();
  } catch (err) {
    if (err.message === "unauthorized") {
      persistToken(null);
      showLoginPanel();
      return;
    }
    console.error("Failed to refresh admin dashboard", err);
  }
}

function renderStats() {
  const stats = adminState.stats || {};
  ridersOnlineEl.textContent = stats.ridersOnlineCount ?? 0;
  driversOnlineEl.textContent = stats.driversOnlineCount ?? 0;
  revenueTodayEl.textContent = formatCurrency(stats.revenueTodayCents || 0);
  ridesTodayEl.textContent = stats.ridesTodayCount ?? 0;
}

function renderActivityFeed() {
  if (!activityFeedEl) return;
  const activity = adminState.activity || [];
  if (!activity.length) {
    activityFeedEl.innerHTML = '<li>No recent activity.</li>';
    return;
  }
  activityFeedEl.innerHTML = activity
    .map((event) => {
      const time = formatTimestamp(event.time);
      if (event.type === "user_signup") {
        return `<li><strong>${time}</strong> · New user signup: ${event.name || "Unknown"} (${event.email || "no email"})</li>`;
      }
      if (event.type === "membership_signup") {
        return `<li><strong>${time}</strong> · Membership: ${event.plan || "plan"} – ${event.email || "no email"}</li>`;
      }
      if (event.type === "membership_request") {
        return `<li><strong>${time}</strong> · U of A request (${event.status || "pending"}): ${event.name || "Unknown"} (${event.email || "no email"})</li>`;
      }
      return `<li><strong>${time}</strong> · ${event.type}</li>`;
    })
    .join("");
}

function renderPendingTable() {
  if (!pendingBodyEl) return;
  const pending = adminState.pending || [];
  if (!pending.length) {
    pendingBodyEl.innerHTML =
      '<tr><td colspan="6" style="text-align:center; padding:16px;">No pending approvals.</td></tr>';
    return;
  }
  pendingBodyEl.innerHTML = pending
    .map((request) => {
      const requestedAt = formatTimestamp(request.requestedAt);
      const studentIdLink = request.studentIdImageUrl
        ? `<a href="${request.studentIdImageUrl}" target="_blank" rel="noreferrer">View ID</a>`
        : "—";
      return `<tr>
        <td>${request.name || "Unknown"}</td>
        <td>${request.email || "—"}</td>
        <td>${request.plan || "uofa_unlimited"}</td>
        <td>${studentIdLink}</td>
        <td>${requestedAt}</td>
        <td>
          <div class="admin-action-row">
            <button class="btn-success" data-approve="${request.id}">Approve</button>
            <button class="btn-danger" data-reject="${request.id}">Reject</button>
          </div>
        </td>
      </tr>`;
    })
    .join("");
}

function renderUserResults(users = []) {
  if (!userDetailsEl) return;
  if (!users.length) {
    userDetailsEl.innerHTML = '<p style="color: var(--muted); margin-top:12px;">No users found.</p>';
    return;
  }
  userDetailsEl.innerHTML = users
    .map((user) => {
      const membershipLine = `${user.membershipType || "basic"} (${user.membershipStatus || "none"})`;
      const expires = user.membershipExpiresAt ? formatTimestamp(user.membershipExpiresAt) : "—";
      return `<div class="admin-user-card">
        <div><strong>Name</strong><br/>${user.name || "Unknown"}</div>
        <div><strong>Email</strong><br/>${user.email || "—"}</div>
        <div><strong>Membership</strong><br/>${membershipLine}</div>
        <div><strong>Expires</strong><br/>${expires}</div>
        <div class="admin-action-row">
          <button class="btn-secondary" data-plan="basic" data-user="${user.userId}">Set Basic</button>
          <button class="btn-secondary" data-plan="uofa_unlimited" data-user="${user.userId}">Set U of A Unlimited</button>
          <button class="btn-secondary" data-plan="nwa_unlimited" data-user="${user.userId}">Set NWA Work Pass</button>
        </div>
      </div>`;
    })
    .join("");
}

async function handleUserSearch() {
  const query = userSearchInput?.value?.trim();
  if (!query) {
    renderUserResults([]);
    return;
  }
  try {
    const response = await adminFetch("search", { query });
    renderUserResults(response?.results || []);
  } catch (err) {
    if (err.message === "unauthorized") {
      persistToken(null);
      showLoginPanel();
      return;
    }
    console.error("User search failed", err);
    renderUserResults([]);
  }
}

async function handlePlanChange(userId, planKey) {
  if (!userId || !planKey) return;
  try {
    await adminFetch("setMembership", { userId, plan: planKey });
    await handleUserSearch();
    await refreshAdminDashboard();
  } catch (err) {
    if (err.message === "unauthorized") {
      persistToken(null);
      showLoginPanel();
      return;
    }
    console.error("Membership update failed", err);
  }
}

async function handlePendingAction(action, requestId) {
  if (!requestId) return;
  const endpoint = action === "approve" ? "approve" : "reject";
  const payload = { requestId };
  if (action === "reject") {
    const reason = prompt("Enter rejection reason", "Missing valid student ID");
    if (reason) {
      payload.reason = reason;
    }
  }
  try {
    await adminFetch(endpoint, payload);
    await refreshAdminDashboard();
  } catch (err) {
    if (err.message === "unauthorized") {
      persistToken(null);
      showLoginPanel();
      return;
    }
    console.error(`Failed to ${action} request`, err);
  }
}

function attachEventListeners() {
  loginButton?.addEventListener("click", (e) => {
    e.preventDefault();
    handleLogin();
  });
  passwordInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      handleLogin();
    }
  });
  userSearchButton?.addEventListener("click", (e) => {
    e.preventDefault();
    handleUserSearch();
  });
  userSearchInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      handleUserSearch();
    }
  });
  userDetailsEl?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const plan = target.dataset.plan;
    const userId = target.dataset.user;
    if (plan && userId) {
      handlePlanChange(userId, plan);
    }
  });
  pendingBodyEl?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.dataset.approve) {
      handlePendingAction("approve", target.dataset.approve);
    } else if (target.dataset.reject) {
      handlePendingAction("reject", target.dataset.reject);
    }
  });
}

(async function init() {
  try {
    const config = await loadConfig();
    adminState.endpoints = buildAdminEndpoints(config);
    if (adminRoot) {
      adminRoot.style.display = "flex";
    }
    attachEventListeners();
    const storedToken = localStorage.getItem(LOCAL_STORAGE_TOKEN_KEY);
    if (storedToken) {
      persistToken(storedToken);
      showDashboard();
      await refreshAdminDashboard();
      scheduleAutoRefresh();
    } else {
      showLoginPanel();
    }
  } catch (err) {
    console.error("Failed to initialize admin dashboard", err);
    if (loginError) {
      loginError.textContent = "Unable to load admin configuration.";
    }
  }
})();

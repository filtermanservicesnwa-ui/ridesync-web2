const APP_CONFIG_URL = "/app-config.json";
const ACTIVE_REFRESH_INTERVAL_MS = 5_000;
const BACKGROUND_REFRESH_INTERVAL_MS = 30_000;
const USERS_REFRESH_MIN_INTERVAL_MS = 15_000;
const FIREBASE_PROJECT_ID = "ride-sync-nwa";
const ADMIN_PAGE_PASSWORD = "Aurora-Verdant-4729";
const ADMIN_PAGE_ACCESS_KEY = "ridesyncAdminPageAccess";
const ADMIN_PAGE_PASSWORD_HEADER = "X-Admin-Page-Pass";
const ADMIN_USERS_PAGE_SIZE = 25;
const AVAILABILITY_DAY_KEYS = [
  "sunday",
  "monday",
  "tuesday",
  "wednesday",
  "thursday",
  "friday",
  "saturday",
];
const AVAILABILITY_DAY_LABELS = {
  sunday: "Sunday",
  monday: "Monday",
  tuesday: "Tuesday",
  wednesday: "Wednesday",
  thursday: "Thursday",
  friday: "Friday",
  saturday: "Saturday",
};

function normalizeDayKey(value) {
  if (typeof value !== "string") {
    return null;
  }
  const key = value.trim().toLowerCase();
  return AVAILABILITY_DAY_KEYS.includes(key) ? key : null;
}

function parseTimeStringToMinutes(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    const normalized = Math.round(value);
    return ((normalized % 1440) + 1440) % 1440;
  }
  if (typeof value !== "string") {
    return null;
  }
  let trimmed = value.trim().toLowerCase();
  if (!trimmed) {
    return null;
  }
  let meridiem = null;
  if (trimmed.endsWith("am") || trimmed.endsWith("pm")) {
    meridiem = trimmed.endsWith("am") ? "am" : "pm";
    trimmed = trimmed.slice(0, -2).trim();
  }
  const parts = trimmed.split(":");
  const hours = Number(parts[0]);
  const minutes = parts[1] !== undefined ? Number(parts[1]) : 0;
  if (!Number.isFinite(hours) || !Number.isFinite(minutes)) {
    return null;
  }
  let normalizedHours = hours;
  if (meridiem) {
    normalizedHours = hours % 12;
    if (meridiem === "pm") {
      normalizedHours += 12;
    }
  }
  normalizedHours = ((normalizedHours % 24) + 24) % 24;
  const clampedMinutes = Math.max(0, Math.min(59, Math.round(minutes)));
  return normalizedHours * 60 + clampedMinutes;
}

function formatAvailabilityTimeLabel(minutes) {
  const normalized = ((Number(minutes) % 1440) + 1440) % 1440;
  const hours = Math.floor(normalized / 60);
  const mins = normalized % 60;
  const period = hours >= 12 ? "PM" : "AM";
  const hour12 = hours % 12 || 12;
  return `${hour12}:${mins.toString().padStart(2, "0")} ${period}`;
}

function formatAvailabilityRangeLabel(startMinutes, endMinutes) {
  if (startMinutes === endMinutes) {
    return "Open 24 hours";
  }
  return `${formatAvailabilityTimeLabel(startMinutes)} – ${formatAvailabilityTimeLabel(endMinutes)}`;
}

function parseAvailabilityWindowEntry(entry) {
  if (!entry) {
    return null;
  }
  let startMinutes = null;
  let endMinutes = null;
  if (typeof entry === "string") {
    const [startRaw, endRaw] = entry.split("-");
    startMinutes = parseTimeStringToMinutes(startRaw);
    endMinutes = parseTimeStringToMinutes(endRaw);
  } else if (typeof entry === "object") {
    const startRaw = entry.start ?? entry.from ?? entry.begin ?? entry.open;
    const endRaw = entry.end ?? entry.to ?? entry.finish ?? entry.close;
    startMinutes = parseTimeStringToMinutes(startRaw);
    endMinutes = parseTimeStringToMinutes(endRaw);
  }
  if (!Number.isInteger(startMinutes) || !Number.isInteger(endMinutes)) {
    return null;
  }
  const normalizedStart = ((startMinutes % 1440) + 1440) % 1440;
  const normalizedEnd = ((endMinutes % 1440) + 1440) % 1440;
  return {
    start: normalizedStart,
    end: normalizedEnd,
    rangeLabel: formatAvailabilityRangeLabel(normalizedStart, normalizedEnd),
  };
}

function computeTimezoneShortName(timezone) {
  if (!timezone) {
    return "";
  }
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone,
      timeZoneName: "short",
      hour: "2-digit",
      minute: "2-digit",
    }).formatToParts(new Date());
    return parts.find((part) => part.type === "timeZoneName")?.value || "";
  } catch (err) {
    console.warn("Failed to resolve timezone abbreviation", err);
    return "";
  }
}

function buildAvailabilityDisplay(config = {}) {
  if (!config || typeof config !== "object") {
    return null;
  }
  const timezone =
    typeof config.timezone === "string" && config.timezone.trim()
      ? config.timezone.trim()
      : "America/Chicago";
  const windowsSource = config.windows || config.days || config.schedule || config.hours || {};
  const normalizedSource = {};
  Object.entries(windowsSource || {}).forEach(([dayKey, value]) => {
    const normalizedKey = normalizeDayKey(dayKey);
    if (!normalizedKey) {
      return;
    }
    if (Array.isArray(value)) {
      normalizedSource[normalizedKey] = value;
      return;
    }
    if (value === null || value === undefined) {
      normalizedSource[normalizedKey] = [];
      return;
    }
    normalizedSource[normalizedKey] = [value];
  });
  const rows = AVAILABILITY_DAY_KEYS.map((dayKey) => {
    const entries = normalizedSource[dayKey] || [];
    const parsed = entries.map((entry) => parseAvailabilityWindowEntry(entry)).filter(Boolean);
    const label = AVAILABILITY_DAY_LABELS[dayKey] || dayKey;
    if (!parsed.length) {
      return { dayKey, label, text: "Closed" };
    }
    return {
      dayKey,
      label,
      text: parsed.map((win) => win.rangeLabel).join(", "),
    };
  });
  const tzShort = computeTimezoneShortName(timezone);
  return {
    timezone,
    timezoneLabel: tzShort ? `${tzShort} (${timezone})` : timezone,
    rows,
  };
}
const TIMER_API = typeof window !== "undefined" ? window : globalThis;

const adminState = {
  pagePassword: null,
  endpoints: {},
  refreshTimer: null,
  activity: [],
  pending: [],
  stats: null,
  users: [],
  usersPaging: createAdminUsersPagingState(),
  loadingUsers: false,
  availabilityDisplay: null,
  lastUsersRefreshAt: 0,
  refreshInFlight: false,
  visibilityListenerAttached: false,
};

const adminRoot = document.getElementById("adminRoot");
const adminDashboard = document.getElementById("adminDashboard");
const adminAccessGate = document.getElementById("adminAccessGate");
const adminAccessPasswordInput = document.getElementById("adminAccessPasswordInput");
const adminAccessButton = document.getElementById("adminAccessButton");
const adminAccessError = document.getElementById("adminAccessError");
const ridersOnlineEl = document.getElementById("adminRidersOnlineCount");
const driversOnlineEl = document.getElementById("adminDriversOnlineCount");
const revenueTodayEl = document.getElementById("adminRevenueToday");
const ridesTodayEl = document.getElementById("adminRidesToday");
const activityFeedEl = document.getElementById("adminActivityFeed");
const pendingBodyEl = document.getElementById("adminUofaPendingBody");
const userSearchInput = document.getElementById("adminUserSearchInput");
const userSearchButton = document.getElementById("adminUserSearchButton");
const userDetailsEl = document.getElementById("adminUserDetails");
const usersTableBody = document.getElementById("adminUsersTableBody");
const usersPrevButton = document.getElementById("adminUsersPrevButton");
const usersNextButton = document.getElementById("adminUsersNextButton");
const usersRefreshButton = document.getElementById("adminUsersRefreshButton");
const usersPageStatus = document.getElementById("adminUsersPageStatus");
const adminHoursList = document.getElementById("adminHoursList");
const adminHoursTimezone = document.getElementById("adminHoursTimezone");
let adminAppReady = false;
let adminAppInitializing = false;

function ensureAdminRootVisible() {
  if (adminRoot && adminRoot.style.display === "none") {
    adminRoot.style.display = "flex";
  }
}

function createAdminUsersPagingState() {
  return {
    currentToken: null,
    nextToken: null,
    prevTokens: [],
    pageSize: ADMIN_USERS_PAGE_SIZE,
    loaded: false,
  };
}

function resetAdminUsersListState() {
  adminState.users = [];
  adminState.usersPaging = createAdminUsersPagingState();
  adminState.lastUsersRefreshAt = 0;
  renderUsersTable();
  renderUsersPagination();
}

function readStoredPagePassword() {
  try {
    const rawValue = sessionStorage.getItem(ADMIN_PAGE_ACCESS_KEY);
    if (!rawValue) {
      return null;
    }
    const parsed = JSON.parse(rawValue);
    if (parsed && typeof parsed.password === "string" && parsed.password.length) {
      return parsed.password;
    }
  } catch (err) {
    return null;
  }
  return null;
}

function hasPageAccess() {
  const storedPassword = readStoredPagePassword();
  if (storedPassword) {
    adminState.pagePassword = storedPassword;
    return true;
  }
  return false;
}

function rememberPagePassword(password) {
  adminState.pagePassword = password;
  try {
    sessionStorage.setItem(
      ADMIN_PAGE_ACCESS_KEY,
      JSON.stringify({
        password,
        grantedAt: Date.now(),
      })
    );
  } catch (err) {
    // Ignore storage errors.
  }
}

function clearStoredPagePassword() {
  try {
    sessionStorage.removeItem(ADMIN_PAGE_ACCESS_KEY);
  } catch (err) {
    // Ignore storage issues.
  }
}

function showAccessGate() {
  ensureAdminRootVisible();
  if (adminAccessGate) {
    adminAccessGate.style.display = "block";
  }
  if (adminAccessPasswordInput) {
    adminAccessPasswordInput.value = "";
  }
  if (adminDashboard) {
    adminDashboard.style.display = "none";
  }
}

function hideAccessGate() {
  if (adminAccessGate) {
    adminAccessGate.style.display = "none";
  }
}

function handleAccessUnlock(event) {
  event?.preventDefault();
  if (!adminAccessPasswordInput) {
    initializeAdminApp();
    return;
  }
  const providedPassword = adminAccessPasswordInput.value?.trim() || "";
  if (providedPassword !== ADMIN_PAGE_PASSWORD) {
    if (adminAccessError) {
      adminAccessError.textContent = "Incorrect page password.";
    }
    return;
  }
  if (adminAccessError) {
    adminAccessError.textContent = "";
  }
  adminAccessPasswordInput.value = "";
  rememberPagePassword(providedPassword);
  hideAccessGate();
  initializeAdminApp();
}

function attachAccessGateListeners() {
  adminAccessButton?.addEventListener("click", (event) => {
    handleAccessUnlock(event);
  });
  adminAccessPasswordInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      handleAccessUnlock(event);
    }
  });
}

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

const HTML_ESCAPE_LOOKUP = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
};
const HTML_ESCAPE_REGEX = /[&<>"']/g;

function escapeHtml(value) {
  const str = value == null ? "" : String(value);
  return str.replace(HTML_ESCAPE_REGEX, (char) => HTML_ESCAPE_LOOKUP[char] || char);
}

function safeDisplay(value, fallback = "—") {
  const base = value == null ? "" : String(value);
  if (!base.trim().length) {
    return escapeHtml(fallback);
  }
  return escapeHtml(base);
}

function renderAvailabilitySchedule() {
  if (!adminHoursList) {
    return;
  }
  const availability = adminState.availabilityDisplay;
  if (!availability || !availability.rows?.length) {
    adminHoursList.innerHTML =
      '<li><span>Schedule</span><span>Not configured</span></li>';
  } else {
    adminHoursList.innerHTML = availability.rows
      .map(
        (row) =>
          `<li><span>${escapeHtml(row.label)}</span><span>${escapeHtml(row.text)}</span></li>`
      )
      .join("");
  }
  if (adminHoursTimezone) {
    if (availability) {
      const tzLabel = availability.timezoneLabel || availability.timezone || "--";
      adminHoursTimezone.textContent = `Timezone: ${tzLabel}`;
    } else {
      adminHoursTimezone.textContent = "Timezone: --";
    }
  }
}

function sanitizeHttpUrl(value) {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  try {
    const parsed = new URL(trimmed, window.location.origin);
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      return parsed.toString();
    }
  } catch (err) {
    return null;
  }
  return null;
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
    listUsers: resolveEndpoint(
      functionsConfig,
      "adminListUsersUrl",
      defaultBase,
      "adminListUsers"
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
  if (adminState.pagePassword) {
    options.headers[ADMIN_PAGE_PASSWORD_HEADER] = adminState.pagePassword;
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

function showDashboard() {
  hideAccessGate();
  ensureAdminRootVisible();
  if (adminDashboard) {
    adminDashboard.style.display = "block";
  }
}

function handleUnauthorizedAccess(message = "Session expired. Please re-enter the password.") {
  clearAdminRefreshTimer();
  adminState.refreshInFlight = false;
  adminState.pagePassword = null;
  clearStoredPagePassword();
  resetAdminUsersListState();
  if (adminDashboard) {
    adminDashboard.style.display = "none";
  }
  if (adminAccessError) {
    adminAccessError.textContent = message;
  }
  showAccessGate();
}

function getRealtimeRefreshDelay() {
  if (typeof document !== "undefined" && document.hidden) {
    return BACKGROUND_REFRESH_INTERVAL_MS;
  }
  return ACTIVE_REFRESH_INTERVAL_MS;
}

function clearAdminRefreshTimer() {
  if (adminState.refreshTimer) {
    TIMER_API.clearTimeout(adminState.refreshTimer);
    adminState.refreshTimer = null;
  }
}

function ensureVisibilityRefreshListener() {
  if (adminState.visibilityListenerAttached || typeof document === "undefined") {
    return;
  }
  document.addEventListener("visibilitychange", () => {
    if (!adminState.pagePassword) {
      return;
    }
    scheduleAutoRefresh({ immediate: document.hidden === false });
  });
  adminState.visibilityListenerAttached = true;
}

async function runRealtimeRefreshCycle(options = {}) {
  if (!adminState.pagePassword || adminState.refreshInFlight) {
    return;
  }
  adminState.refreshInFlight = true;
  try {
    await refreshAdminDashboard();
    const includeUsers = options.includeUsers !== false;
    const now = Date.now();
    const elapsedSinceUsers = now - (adminState.lastUsersRefreshAt || 0);
    const canReloadUsers =
      includeUsers &&
      adminState.usersPaging?.loaded &&
      !adminState.loadingUsers &&
      elapsedSinceUsers >= USERS_REFRESH_MIN_INTERVAL_MS;
    if (canReloadUsers) {
      await loadAdminUsersPage("reload");
    }
  } catch (err) {
    console.error("Realtime refresh failed", err);
  } finally {
    adminState.refreshInFlight = false;
  }
}

function scheduleAutoRefresh(options = {}) {
  clearAdminRefreshTimer();
  if (!adminState.pagePassword) {
    return;
  }
  ensureVisibilityRefreshListener();
  const immediate = options?.immediate === true;
  const delay = immediate ? 0 : getRealtimeRefreshDelay();
  adminState.refreshTimer = TIMER_API.setTimeout(async () => {
    adminState.refreshTimer = null;
    await runRealtimeRefreshCycle();
    scheduleAutoRefresh();
  }, delay);
}

async function refreshAdminDashboard() {
  if (!adminState.pagePassword) {
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
      handleUnauthorizedAccess();
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
      const time = escapeHtml(formatTimestamp(event.time));
      const name = safeDisplay(event.name, "Unknown");
      const email = safeDisplay(event.email, "no email");
      if (event.type === "user_signup") {
        return `<li><strong>${time}</strong> · New user signup: ${name} (${email})</li>`;
      }
      if (event.type === "membership_signup") {
        const plan = safeDisplay(event.plan, "plan");
        return `<li><strong>${time}</strong> · Membership: ${plan} – ${email}</li>`;
      }
      if (event.type === "membership_request") {
        const status = safeDisplay(event.status, "pending");
        return `<li><strong>${time}</strong> · U of A request (${status}): ${name} (${email})</li>`;
      }
      const type = safeDisplay(event.type, "event");
      return `<li><strong>${time}</strong> · ${type}</li>`;
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
      const requestedAt = escapeHtml(formatTimestamp(request.requestedAt));
      const studentIdUrl = sanitizeHttpUrl(request.studentIdImageUrl);
      const studentIdLink = studentIdUrl
        ? `<a href="${escapeHtml(studentIdUrl)}" target="_blank" rel="noreferrer noopener">View ID</a>`
        : "—";
      const name = safeDisplay(request.name, "Unknown");
      const email = safeDisplay(request.email, "—");
      const plan = safeDisplay(request.plan || request.planKey, "uofa_unlimited");
      const requestId = escapeHtml(request.id || request.requestId || "");
      return `<tr>
        <td>${name}</td>
        <td>${email}</td>
        <td>${plan}</td>
        <td>${studentIdLink}</td>
        <td>${requestedAt}</td>
        <td>
          <div class="admin-action-row">
            <button class="btn-success" data-approve="${requestId}">Approve</button>
            <button class="btn-danger" data-reject="${requestId}">Reject</button>
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
      const membershipLine = escapeHtml(
        `${user.membershipType || "basic"} (${user.membershipStatus || "none"})`
      );
      const expires = user.membershipExpiresAt
        ? escapeHtml(formatTimestamp(user.membershipExpiresAt))
        : "—";
      const name = safeDisplay(user.name, "Unknown");
      const email = safeDisplay(user.email, "—");
      const userIdAttr = escapeHtml(user.userId || "");
      return `<div class="admin-user-card">
        <div><strong>Name</strong><br/>${name}</div>
        <div><strong>Email</strong><br/>${email}</div>
        <div><strong>Membership</strong><br/>${membershipLine}</div>
        <div><strong>Expires</strong><br/>${expires}</div>
        <div class="admin-action-row">
          <button class="btn-secondary" data-plan="basic" data-user="${userIdAttr}">Set Basic</button>
          <button class="btn-secondary" data-plan="uofa_unlimited" data-user="${userIdAttr}">Set U of A Unlimited</button>
          <button class="btn-secondary" data-plan="nwa_unlimited" data-user="${userIdAttr}">Set NWA Work Pass</button>
        </div>
      </div>`;
    })
    .join("");
}

function renderUsersTable() {
  if (!usersTableBody) return;
  const rows = adminState.users || [];
  if (!rows.length) {
    const message = adminState.loadingUsers
      ? "Loading users..."
      : adminState.usersPaging?.loaded
      ? "No users in this page."
      : "No users loaded.";
    usersTableBody.innerHTML = `<tr><td colspan="6" style="text-align:center; padding:16px;">${escapeHtml(
      message
    )}</td></tr>`;
    return;
  }
  usersTableBody.innerHTML = rows
    .map((user) => {
      const membershipLine = escapeHtml(
        `${user.membershipType || "basic"} (${user.membershipStatus || "none"})`
      );
      const expires = user.membershipExpiresAt
        ? escapeHtml(formatTimestamp(user.membershipExpiresAt))
        : "—";
      const name = safeDisplay(user.name, "Unknown");
      const email = safeDisplay(user.email, "—");
      const verified = user.uofaVerified ? "Yes" : "No";
      const userIdAttr = escapeHtml(user.userId || "");
      return `<tr>
        <td>${name}</td>
        <td>${email}</td>
        <td>${membershipLine}</td>
        <td>${expires}</td>
        <td>${escapeHtml(verified)}</td>
        <td>
          <div class="admin-action-row compact">
            <button class="btn-secondary btn-compact" data-plan="basic" data-user="${userIdAttr}">Basic</button>
            <button class="btn-secondary btn-compact" data-plan="uofa_unlimited" data-user="${userIdAttr}">U of A</button>
            <button class="btn-secondary btn-compact" data-plan="nwa_unlimited" data-user="${userIdAttr}">NWA</button>
          </div>
        </td>
      </tr>`;
    })
    .join("");
}

function renderUsersPagination() {
  const paging = adminState.usersPaging || createAdminUsersPagingState();
  if (usersPrevButton) {
    usersPrevButton.disabled = adminState.loadingUsers || paging.prevTokens.length === 0;
  }
  if (usersNextButton) {
    usersNextButton.disabled = adminState.loadingUsers || !paging.nextToken;
  }
  if (!usersPageStatus) {
    return;
  }
  if (adminState.loadingUsers) {
    usersPageStatus.textContent = "Loading users...";
    return;
  }
  if (!paging.loaded) {
    usersPageStatus.textContent = "No users loaded.";
    return;
  }
  const count = adminState.users?.length || 0;
  if (!count) {
    usersPageStatus.textContent = "No users in this page.";
    return;
  }
  const completedPages = paging.prevTokens.length;
  const startIndex = completedPages * paging.pageSize + 1;
  const endIndex = startIndex + count - 1;
  usersPageStatus.textContent = `Showing ${startIndex}-${endIndex} (${count} users)`;
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
      handleUnauthorizedAccess();
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
    await loadAdminUsersPage("reload");
  } catch (err) {
    if (err.message === "unauthorized") {
      handleUnauthorizedAccess();
      return;
    }
    console.error("Membership update failed", err);
  }
}

function handleMembershipActionClick(target) {
  if (!(target instanceof HTMLElement)) return;
  const plan = target.dataset.plan;
  const userId = target.dataset.user;
  if (plan && userId) {
    handlePlanChange(userId, plan);
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
      handleUnauthorizedAccess();
      return;
    }
    console.error(`Failed to ${action} request`, err);
  }
}

async function loadAdminUsersPage(action = "initial") {
  if (!adminState.pagePassword) {
    return;
  }
  const paging = adminState.usersPaging || createAdminUsersPagingState();
  adminState.usersPaging = paging;
  if (adminState.loadingUsers && action !== "reload") {
    return;
  }
  let pageToken = null;
  let pushedToken = false;
  let poppedTokenValue;
  let poppedTokenActive = false;
  if (action === "next") {
    if (!paging.nextToken) {
      return;
    }
    paging.prevTokens.push(paging.currentToken || null);
    pushedToken = true;
    pageToken = paging.nextToken;
  } else if (action === "prev") {
    if (!paging.prevTokens.length) {
      return;
    }
    poppedTokenValue = paging.prevTokens.pop();
    poppedTokenActive = true;
    pageToken = poppedTokenValue || null;
  } else if (action === "reload") {
    pageToken = paging.currentToken || null;
  } else {
    paging.prevTokens = [];
    paging.currentToken = null;
    paging.nextToken = null;
    paging.loaded = false;
    pageToken = null;
  }
  const shouldClearRows = action !== "reload";
  if (shouldClearRows) {
    adminState.users = [];
  }
  adminState.loadingUsers = true;
  renderUsersTable();
  renderUsersPagination();
  try {
    const body = { pageSize: paging.pageSize };
    if (pageToken) {
      body.pageToken = pageToken;
    }
    const response = await adminFetch("listUsers", body);
    adminState.users = response?.users || [];
    adminState.lastUsersRefreshAt = Date.now();
    paging.currentToken = pageToken || null;
    paging.nextToken = response?.nextPageToken || null;
    paging.loaded = true;
    adminState.loadingUsers = false;
    renderUsersTable();
    renderUsersPagination();
  } catch (err) {
    adminState.loadingUsers = false;
    if (action === "next" && pushedToken) {
      paging.prevTokens.pop();
    } else if (action === "prev" && poppedTokenActive) {
      paging.prevTokens.push(poppedTokenValue);
    }
    renderUsersTable();
    renderUsersPagination();
    if (err.message === "unauthorized") {
      handleUnauthorizedAccess();
      return;
    }
    console.error("Failed to load admin users", err);
  }
}

function attachEventListeners() {
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
    handleMembershipActionClick(event.target);
  });
  usersTableBody?.addEventListener("click", (event) => {
    handleMembershipActionClick(event.target);
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
  usersPrevButton?.addEventListener("click", (event) => {
    event.preventDefault();
    loadAdminUsersPage("prev");
  });
  usersNextButton?.addEventListener("click", (event) => {
    event.preventDefault();
    loadAdminUsersPage("next");
  });
  usersRefreshButton?.addEventListener("click", (event) => {
    event.preventDefault();
    const action = adminState.usersPaging?.loaded ? "reload" : "initial";
    loadAdminUsersPage(action);
  });
}

async function initializeAdminApp() {
  if (adminAppReady) {
    if (adminState.pagePassword) {
      showDashboard();
      const userPageAction = adminState.usersPaging?.loaded ? "reload" : "initial";
      await Promise.all([refreshAdminDashboard(), loadAdminUsersPage(userPageAction)]);
      scheduleAutoRefresh();
    }
    return;
  }
  if (adminAppInitializing) {
    return;
  }
  adminAppInitializing = true;
  try {
    const config = await loadConfig();
    adminState.endpoints = buildAdminEndpoints(config);
    adminState.availabilityDisplay = buildAvailabilityDisplay(config?.availability || {});
    renderAvailabilitySchedule();
    ensureAdminRootVisible();
    attachEventListeners();
    if (adminState.pagePassword) {
      showDashboard();
      await Promise.all([refreshAdminDashboard(), loadAdminUsersPage("initial")]);
      scheduleAutoRefresh();
    } else {
      showAccessGate();
    }
    adminAppReady = true;
  } catch (err) {
    console.error("Failed to initialize admin dashboard", err);
    if (adminAccessError) {
      adminAccessError.textContent = "Unable to load admin configuration.";
    }
  } finally {
    adminAppInitializing = false;
  }
}

function bootstrapAdminPage() {
  attachAccessGateListeners();
  if (hasPageAccess()) {
    hideAccessGate();
    initializeAdminApp();
  } else {
    showAccessGate();
  }
}

bootstrapAdminPage();

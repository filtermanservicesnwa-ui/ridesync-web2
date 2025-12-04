// public/firebase-messaging-sw.js

importScripts("https://www.gstatic.com/firebasejs/10.13.0/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/10.13.0/firebase-messaging-compat.js");

let messaging = null;

function loadAppConfig() {
  if (self.__rideSyncConfigPromise) {
    return self.__rideSyncConfigPromise;
  }

  self.__rideSyncConfigPromise = fetch("/app-config.json", {
    cache: "reload"
  })
    .then((res) => {
      if (!res.ok) {
        throw new Error(`Failed to load app config: ${res.status}`);
      }
      return res.json();
    })
    .catch((err) => {
      console.error("[firebase-messaging-sw] Failed to load app config", err);
      throw err;
    });

  return self.__rideSyncConfigPromise;
}

(async () => {
  try {
    const config = await loadAppConfig();
    firebase.initializeApp(config.firebaseConfig || {});
    messaging = firebase.messaging();

    messaging.onBackgroundMessage((payload) => {
      console.log("[firebase-messaging-sw] Background message", payload);

      const notificationTitle = payload.notification?.title || "New Ride";
      const notificationOptions = {
        body: payload.notification?.body || "You have a new RideSync request.",
        icon: "/icons/icon-192.png",
        badge: "/icons/icon-192.png",
        data: {
          click_action: "https://ridesync.live/driver.html" // Ensure driver pushes land on the Netlify domain
        }
      };

      self.registration.showNotification(notificationTitle, notificationOptions);
    });
  } catch (err) {
    console.error("[firebase-messaging-sw] Initialization error:", err);
  }
})();

self.addEventListener("notificationclick", function (event) {
  event.notification.close();
  const targetUrl = "https://ridesync.live/driver.html"; // Mirror the driver portal domain used above

  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if (client.url.includes("/driver.html") && "focus" in client) {
          return client.focus();
        }
      }
      if (clients.openWindow) {
        return clients.openWindow(targetUrl);
      }
    })
  );
});

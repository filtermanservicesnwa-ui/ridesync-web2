// public/firebase-messaging-sw.js

importScripts("https://www.gstatic.com/firebasejs/10.13.0/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/10.13.0/firebase-messaging-compat.js");

firebase.initializeApp({
  apiKey: "AIzaSyA4BPyi7sDZtcsOZM-FDzl2DQ61wUTcejo",
  authDomain: "ride-sync-nwa.firebaseapp.com",
  projectId: "ride-sync-nwa",
  storageBucket: "ride-sync-nwa.firebasestorage.app",
  messagingSenderId: "221636626778",
  appId: "1:221636626778:web:fe1afd1f95a16747898b63",
});

const messaging = firebase.messaging();

messaging.onBackgroundMessage((payload) => {
  console.log("[firebase-messaging-sw] Background message", payload);

  const notificationTitle = payload.notification?.title || "New Ride";
  const notificationOptions = {
    body: payload.notification?.body || "You have a new RideSync request.",
    icon: "/icons/icon-192.png",
    badge: "/icons/icon-192.png",
    data: {
      click_action: "https://ride-sync-nwa.web.app/driver.html"
    }
  };

  self.registration.showNotification(notificationTitle, notificationOptions);
});

self.addEventListener("notificationclick", function (event) {
  event.notification.close();
  const targetUrl = "https://ride-sync-nwa.web.app/driver.html";

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

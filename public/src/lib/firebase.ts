import { initializeApp, getApp, getApps } from "firebase/app";
import { getAuth } from "firebase/auth";
import { getFirestore } from "firebase/firestore";

const firebaseConfig = {
  apiKey: "AIzaSyA4BPyi7sDZtcsOZM-FDzl2DQ61wUTcejo",
  authDomain: "ride-sync-nwa.firebaseapp.com",
  projectId: "ride-sync-nwa",
  storageBucket: "ride-sync-nwa.firebasestorage.app",
  messagingSenderId: "221636626778",
  appId: "1:221636626778:web:fe1afd1f95a16747898b63"
};

// Ensure Firebase initializes ONLY once
const app = getApps().length ? getApp() : initializeApp(firebaseConfig);

export const auth = getAuth(app);
export const db = getFirestore(app);
export default app;

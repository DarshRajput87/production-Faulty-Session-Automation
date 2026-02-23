// =====================================================
// 📦 IMPORTS
// =====================================================
import axios from "axios";
import fs from "fs";
import path from "path";
import XLSX from "xlsx";
import nodemailer from "nodemailer";
import os from "os";
import { MongoClient, ObjectId } from "mongodb";
import { PARTY_CONFIG } from "./config/partyConfig.js";

// =====================================================
// ⚙ CONFIG
// =====================================================
  const TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI2NDJlZTBkNmU1MmIzYjg1MWNmN2MxMjkiLCJhdXRoVG9rZW5WZXJzaW9uIjoidjEiLCJpYXQiOjE3NzEzMTc4MzgsImV4cCI6MTc3MjYxMzgzOCwidHlwZSI6ImFjY2VzcyJ9.qO5zt2MqTSSzuSLV8muFoO6ePafkr1sArArPhXISttQ";
const API_URL = "https://appapi.chargecloud.net/v1//report/emspFaultyBookings";
  const MONGO_URI ="mongodb+srv://IT_INTERN:ITINTERN123@cluster1.0pycd.mongodb.net/chargezoneprod";


const todayFolder = new Date().toISOString().split("T")[0];

const reportDir = path.join("reports", todayFolder);
const masterDir = "MasterData";
const lockFile = "process.lock";

[reportDir, masterDir].forEach(d => {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
});

const excelPath = path.join(reportDir, "emspFaultyBookings.xlsx");

// =====================================================
// 🧾 LOGGER
// =====================================================
function log(step, msg) {
  console.log(`[${new Date().toISOString()}] [${step}] ${msg}`);
}

// =====================================================
// 🔒 LOCK (PM2 SAFE)
// =====================================================
function acquireLock() {
  if (fs.existsSync(lockFile)) {
    log("LOCK", "Another instance already running");
    process.exit(0);
  }
  fs.writeFileSync(lockFile, String(process.pid));
}

function releaseLock() {
  if (fs.existsSync(lockFile)) fs.unlinkSync(lockFile);
}

// =====================================================
// 🔁 RETRY WRAPPER
// =====================================================
async function retry(fn, retries = 3, delay = 3000) {
  try {
    return await fn();
  } catch (err) {
    if (retries <= 0) throw err;
    log("RETRY", `Retrying... (${retries})`);
    await new Promise(r => setTimeout(r, delay));
    return retry(fn, retries - 1, delay);
  }
}

// =====================================================
// 📧 MAIL
// =====================================================
const transporter = nodemailer.createTransport({
  service: "gmail",
   auth: {
      user: "darshraj3104@gmail.com",
      pass: "ddxg ddtb fiiz mygh"
    }
});

// =====================================================
// 📨 MAIL CONTENT BUILDER (PRO TRACKING)
// =====================================================
function buildMailText({ type, partyId, batch, count }) {

  let actionLine = "";

  if (type === "Notification") {
    actionLine =
      "New faulty charging sessions have been identified. Kindly review and resolve them at the earliest to avoid billing or reconciliation issues.";
  }

  if (type === "Reminder1") {
    actionLine =
      "This is a gentle reminder that some faulty charging sessions are still pending resolution. Timely action helps ensure accurate settlement and reporting.";
  }

  if (type === "FinalReminder") {
    actionLine =
      "Final reminder: Immediate action is required for the pending faulty charging sessions to prevent operational or financial discrepancies.";
  }

  return `
Hello Team,

This is an automated communication from the Chargezone EMSP Monitoring System.

The attached report contains charging sessions that are currently marked as *faulty* or require attention based on EMSP validation checks. These sessions may need verification, correction, or closure from your side to ensure accurate billing, data consistency, and seamless platform operations.

--------------------------------------------------
Partner Name          : ${partyId}
Report Date           : ${batch}
Total Faulty Sessions : ${count}
--------------------------------------------------

${actionLine}

Please review the attached report and take the necessary actions.  
If the sessions have already been resolved, kindly ignore this notification.

Regards,  
Chargezone EMSP Monitoring System
`;
}

// =====================================================
// 📎 BUFFER ATTACHMENT (REMOVE SENSITIVE COLUMNS)
// =====================================================
function createMailBuffer(rows) {

  const removedColumns = new Set([
    "Vehicle ID",
    "Vehicle Make",
    "Vehicle Model",
    "VIN Number",
    "Max Charging Capacity",
    "User Name",
    "User Phone"
  ]);

  // ================= CLEAN DATA =================
  const cleaned = rows.map(row => {

    const obj = {};

    Object.keys(row).forEach(rawKey => {
      const key = rawKey.trim();
      if (!removedColumns.has(key)) {
        obj[key] = row[rawKey];
      }
    });

    return obj;
  });

  // ================= CREATE SHEET =================
  const ws = XLSX.utils.json_to_sheet(cleaned);

  // =====================================================
  // ✅ FORCE SR NO COLUMN (ALWAYS START FROM 1)
  // =====================================================
  XLSX.utils.sheet_add_aoa(ws, [["Sr No."]], { origin: "A1" });

  for (let i = 0; i < cleaned.length; i++) {
    XLSX.utils.sheet_add_aoa(ws, [[i + 1]], { origin: `A${i + 2}` });
  }

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Faulty");

  return XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
}

// =====================================================
// 🗄 MASTER
// =====================================================
function loadMaster(partyId) {

  const filePath = path.join(masterDir, `${partyId}_Master.xlsx`);

  if (!fs.existsSync(filePath))
    return { data: [], path: filePath };

  const wb = XLSX.readFile(filePath);
  const data =
    XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);

  return { data, path: filePath };
}

function saveMaster(data, filePath) {

  const reIndexed = data.map((row, i) => {
    const { "Sr No.": _, ...rest } = row;
    return { "Sr No.": i + 1, ...rest };
  });

  const ws = XLSX.utils.json_to_sheet(reIndexed);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Tracking");

  XLSX.writeFile(wb, filePath);
}

// =====================================================
// 🗃 DB
// =====================================================
const client = new MongoClient(MONGO_URI);
let bookingCollection;

async function connectDB() {
  await client.connect();
  bookingCollection = client.db("chargezoneprod").collection("chargerbookings");
  log("DB", "Connected");
}

async function fetchBookingsBulk(ids) {

  const validIds = ids
    .filter(id => ObjectId.isValid(id))
    .map(id => new ObjectId(id));

  const docs = await bookingCollection.find({
    _id: { $in: validIds }
  }).toArray();

  const map = new Map();
  docs.forEach(d => map.set(String(d._id), d));
  return map;
}

// =====================================================
// 📥 DOWNLOAD EXCEL
// =====================================================
// ================= DOWNLOAD EXCEL (MONTH-TO-DATE IST) =================
async function downloadExcel() {

  const now = new Date();

  // 👉 Get IST parts safely
  const istParts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  }).formatToParts(now).reduce((acc, p) => {
    if (p.type !== "literal") acc[p.type] = Number(p.value);
    return acc;
  }, {});

  // =====================================================
  // ✅ FROM = 1st of current month 00:00 IST → UTC
  // =====================================================
  const fromISO = new Date(Date.UTC(
    istParts.year,
    istParts.month - 1,
    1,      // 👈 First day
    -5,     // IST offset
    -30,
    0
  )).toISOString();

  // =====================================================
  // ✅ TO = CURRENT IST → UTC
  // =====================================================
  const toISO = new Date(Date.UTC(
    istParts.year,
    istParts.month - 1,
    istParts.day,
    istParts.hour - 5,
    istParts.minute - 30,
    istParts.second
  )).toISOString();

  log("API", `Downloading IST Month Range → ${fromISO} → ${toISO}`);

  const response = await retry(() =>
    axios.post(
      API_URL,
      {
        payment_status: "action_required",
        excel: true,
        from: fromISO,
        to: toISO
      },
      {
        responseType: "arraybuffer",
        headers: {
          authorization: `Bearer ${TOKEN}`,
          "content-type": "application/json"
        }
      }
    )
  );

  fs.writeFileSync(excelPath, response.data);

  log("API", "Excel Downloaded (Month-to-Date)");
}

// =====================================================
// 🔍 FAULT CHECK
// =====================================================
function isFaulty(doc, partyId) {

  const party = PARTY_CONFIG[partyId];
  if (!party) return false;

  const credential = doc.ocpiCredential
    ? String(doc.ocpiCredential)
    : null;

  if (!party.ocpiCredentials.includes(credential))
    return false;

  return (
    doc.is_ocpi_based_booking &&
    doc.is_emsp_based_booking &&
    !doc.invoice &&
    Array.isArray(doc.faulty_booking_reason) &&
    doc.faulty_booking_reason.length > 0 &&
    doc.payment_status === "action_required"
  );
}

// =====================================================
// 🧠 CORE
// =====================================================
// =====================================================
// 🧠 CORE (OLDER WORKING LOGIC RESTORED)
// =====================================================
// =====================================================
// 🧠 CORE (THREAD SAFE DAILY VERSION)
// =====================================================
async function reconcileAndProcess() {
 const REMINDER_DELAY = 20 * 60 * 1000;   // 20 minutes
 const FINAL_DELAY = 20 * 60 * 1000;      // 20 minutes
 const TIME_BUFFER = 60 * 1000; 

  if (!fs.existsSync(excelPath)) {
    log("ERROR", "Excel missing");
    return;
  }

  const workbook = XLSX.readFile(excelPath);
  const jsonData =
    XLSX.utils.sheet_to_json(
      workbook.Sheets[workbook.SheetNames[0]],
      { range: 2 }
    );

  const partyMap = {};

  jsonData.forEach(r => {
    const partyId = String(r["Party ID"]).trim();
    if (!partyId) return;
    if (!partyMap[partyId]) partyMap[partyId] = [];
    partyMap[partyId].push(r);
  });

  // =====================================================
  // 🔁 LOOP PER PARTY
  // =====================================================
  for (const [partyId, rows] of Object.entries(partyMap)) {

    log("PROCESS", `Processing ${partyId}`);

    const bookingIds =
      rows.map(r => r["Authorization Reference"]);

    const bookingMap =
      await fetchBookingsBulk(bookingIds);

    const dbFaultyRows =
      rows.filter(r => {
        const doc =
          bookingMap.get(
            String(r["Authorization Reference"])
          );
        return doc &&
          isFaulty(doc, partyId);
      });

    const { data: masterData, path: masterPath } =
      loadMaster(partyId);

    const existingIds =
      masterData.map(r => r["Authorization Reference"]);

    const todayIds =
      dbFaultyRows.map(r => r["Authorization Reference"]);

    // =====================================================
    // ✅ UPDATE STILL_EXIST
    // =====================================================
    masterData.forEach(row => {
      row["Still_Exist"] =
        todayIds.includes(row["Authorization Reference"])
          ? "YES"
          : "NO";

      row["Still_Exist_Timestamp"] =
        new Date().toISOString();
    });

    // =====================================================
    // 🆕 NEW IDS → NEW THREAD (TODAY ONLY)
    // =====================================================
    const newRows =
      dbFaultyRows.filter(r =>
        !existingIds.includes(
          r["Authorization Reference"])
      );

    if (newRows.length) {

      const baseSubject =
        `[AUTO-Notification] Faulty Sessions - ${partyId} - ${todayFolder}`;

      const buffer = createMailBuffer(newRows);

      const info =
        await transporter.sendMail({
          to: PARTY_CONFIG[partyId].emails.join(","),
          subject: baseSubject,
          text: buildMailText({
            type: "Notification",
            partyId,
            batch: todayFolder,
            count: newRows.length
          }),
          attachments: [{
            filename: `${partyId}_FaultySessions.xlsx`,
            content: buffer
          }]
        });

      newRows.forEach(r => {
        masterData.push({
          ...r,
          "Batch_Date": todayFolder,
          "Thread_ID": info.messageId,
          "Notification_Timestamp":
            new Date().toISOString(),
          "Reminder1_Timestamp": "",
          "FinalReminder_Timestamp": "",
          "Still_Exist": "YES",
          "Still_Exist_Timestamp": ""
        });
      });

      log("MAIL", `Notification sent for ${partyId}`);
    }

    // =====================================================
    // 🔁 REMINDER ENGINE (PER BATCH = PER DAY THREAD)
    // =====================================================
    const batches =
      [...new Set(masterData.map(r => r["Batch_Date"]))];

    for (const batch of batches) {

      const batchRows =
        masterData.filter(r =>
          r["Batch_Date"] === batch &&
          r["Still_Exist"] === "YES"
        );

      if (!batchRows.length) continue;

      const firstRow = batchRows[0];

      const baseSubject =
        `[AUTO-Notification] Faulty Sessions - ${partyId} - ${batch}`;

      const now = Date.now();
      const notifTime =
        new Date(firstRow["Notification_Timestamp"]).getTime();

      const rem1Time =
        firstRow["Reminder1_Timestamp"]
          ? new Date(firstRow["Reminder1_Timestamp"]).getTime()
          : null;

      // ================= REMINDER 1 =================
      if (!firstRow["Reminder1_Timestamp"] &&
          now - notifTime >= REMINDER_DELAY - TIME_BUFFER) {

        const buffer = createMailBuffer(batchRows);

        await transporter.sendMail({
          to: PARTY_CONFIG[partyId].emails.join(","),
          subject: `Re: ${baseSubject}`,
          text: buildMailText({
            type: "Reminder1",
            partyId,
            batch,
            count: batchRows.length
          }),
          headers: {
            "In-Reply-To": firstRow["Thread_ID"],
            "References": firstRow["Thread_ID"]
          },
          attachments: [{
            filename: `${partyId}_FaultySessions.xlsx`,
            content: buffer
          }]
        });

        batchRows.forEach(r =>
          r["Reminder1_Timestamp"] =
            new Date().toISOString()
        );

        log("MAIL", `Reminder1 sent for ${partyId} - ${batch}`);
      }

      // ================= FINAL REMINDER =================
               // 1 minute tolerance
      if (rem1Time &&
          !firstRow["FinalReminder_Timestamp"] &&
         now - rem1Time >= FINAL_DELAY - TIME_BUFFER) {

        const buffer = createMailBuffer(batchRows);

        await transporter.sendMail({
          to: PARTY_CONFIG[partyId].emails.join(","),
          subject: `Re: ${baseSubject}`,
          text: buildMailText({
            type: "FinalReminder",
            partyId,
            batch,
            count: batchRows.length
          }),
          headers: {
            "In-Reply-To": firstRow["Thread_ID"],
            "References": firstRow["Thread_ID"]
          },
          attachments: [{
            filename: `${partyId}_FaultySessions.xlsx`,
            content: buffer
          }]
        });

        batchRows.forEach(r =>
          r["FinalReminder_Timestamp"] =
            new Date().toISOString()
        );

        log("MAIL", `FinalReminder sent for ${partyId} - ${batch}`);
      }
    }

    saveMaster(masterData, masterPath);
  }
}

// =====================================================
// ▶ RUN
// =====================================================
async function run() {

  acquireLock();
  log("SYSTEM", "Started");

  try {
    await connectDB();
    await downloadExcel();
    await reconcileAndProcess();
    log("SYSTEM", "Completed Successfully");

  } catch (error) {

    log("FATAL ERROR", error.message);
    console.error(error);

  } finally {

    releaseLock();

    try {
      await client.close();
      log("DB", "Connection Closed");
    } catch {}

    log("SYSTEM", "Auto Stopped");
    process.exit(0);
  }
}

run();

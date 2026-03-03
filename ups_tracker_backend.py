"""
Hyper Tracker v5 — Multi-Carrier (UPS + bpost + PostNL)
─────────────────────────────────────────────────────────
UPS   : Tracking + Time In Transit + Track Alert (webhook)
bpost : Public Track API (track.bpost.cloud)
PostNL: Public Track API (jouw.postnl.nl)
Push  : SSE + Telegram
"""

import os, json, time, uuid, base64, traceback, re, math, asyncio
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse

load_dotenv()
app = FastAPI(title="Rayhann_Explorer Correos")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

CLIENT_ID = os.environ.get("UPS_CLIENT_ID", "").strip()
CLIENT_SECRET = os.environ.get("UPS_CLIENT_SECRET", "").strip()
SHIPPER_NUMBER = os.environ.get("UPS_SHIPPER_NUMBER", "").strip()
UPS_ENV = os.environ.get("UPS_ENV", "prod").strip().lower()
if UPS_ENV not in ("prod", "cie"): UPS_ENV = "prod"
API_BASE = "https://onlinetools.ups.com/api" if UPS_ENV == "prod" else "https://wwwcie.ups.com/api"
OAUTH_URL = "https://onlinetools.ups.com/security/v1/oauth/token" if UPS_ENV == "prod" else "https://wwwcie.ups.com/security/v1/oauth/token"
WEBHOOK_URL = os.environ.get("TRACK_ALERT_WEBHOOK_URL", "").strip()
WEBHOOK_CRED = os.environ.get("TRACK_ALERT_WEBHOOK_CREDENTIAL", "").strip()
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# ══════════════════════════════════════════════════════════════
# CARRIER DETECTION
# ══════════════════════════════════════════════════════════════
def detect_carrier(tn):
    """Auto-detect carrier from tracking number format."""
    tn = tn.strip()
    # UPS: starts with 1Z
    if re.match(r'^1Z[A-Z0-9]{6}\d{10}$', tn, re.IGNORECASE):
        return "ups"
    if tn.upper().startswith("1Z"):
        return "ups"
    # bpost: 24 digits starting with 3232 or 3299, or 13-char (2 letters + 9 digits + 2 letters)
    if re.match(r'^(3232|3299)\d{20}$', tn):
        return "bpost"
    if re.match(r'^[A-Z]{2}\d{9}[A-Z]{2}$', tn, re.IGNORECASE):
        return "bpost"
    # PostNL: 3S + country + 13 digits, or 13-digit barcode
    if re.match(r'^3S[A-Z]{2}[A-Z0-9]{9,20}$', tn, re.IGNORECASE):
        return "postnl"
    if re.match(r'^\d{13,14}$', tn):
        return "postnl"  # Could also be bpost, but PostNL is more common for NL
    # CD/CE bpost international
    if re.match(r'^(CD|CE|CZ|EE|RR|RI)\d{9}BE$', tn, re.IGNORECASE):
        return "bpost"
    # LS/LY PostNL
    if re.match(r'^(LS|LY|NL)\d+$', tn, re.IGNORECASE):
        return "postnl"
    # Default: try UPS
    return "ups"


# ══════════════════════════════════════════════════════════════
# UPS
# ══════════════════════════════════════════════════════════════
_tc = {"token": None, "exp": 0}
def get_token():
    if _tc["token"] and time.time() < _tc["exp"] - 30: return _tc["token"]
    if not CLIENT_ID or not CLIENT_SECRET: raise HTTPException(500, "UPS credentials manquants")
    b = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(OAUTH_URL, headers={"Authorization": f"Basic {b}", "Content-Type": "application/x-www-form-urlencoded"}, data={"grant_type": "client_credentials"}, timeout=30)
    if r.status_code != 200: raise HTTPException(502, f"OAuth: {r.status_code}")
    d = r.json(); _tc["token"] = d["access_token"]; _tc["exp"] = time.time() + int(d.get("expires_in", 3600))
    return _tc["token"]

def _h(tok):
    return {"Authorization": f"Bearer {tok}", "Accept": "application/json", "Content-Type": "application/json", "transId": str(uuid.uuid4()), "transactionSrc": "hyper-tracker"}

SVC_MAP = {
    "001": "UPS Next Day Air", "002": "UPS 2nd Day Air", "003": "UPS Ground",
    "007": "UPS Express", "008": "UPS Expedited", "011": "UPS Standard",
    "012": "UPS 3 Day Select", "013": "UPS Next Day Air Saver",
    "014": "UPS Next Day Air Early", "054": "UPS Express Plus",
    "059": "UPS 2nd Day Air A.M.", "065": "UPS Saver",
    "070": "UPS Access Point Economy", "093": "UPS SurePost",
}

def detect_service(tracking_data):
    sh = tracking_data.get("trackResponse", {}).get("shipment", [{}])[0]
    pk = sh.get("package", [{}])
    if isinstance(pk, list): pk = pk[0] if pk else {}
    svc = sh.get("service", {})
    if isinstance(svc, dict) and svc.get("description"):
        return {"code": svc.get("code", ""), "description": svc["description"]}
    if isinstance(svc, dict) and svc.get("code"):
        return {"code": svc["code"], "description": SVC_MAP.get(svc["code"], f"Service {svc['code']}")}
    return {"code": "?", "description": ""}

def call_tracking(tok, tn):
    r = requests.get(f"{API_BASE}/track/v1/details/{tn}",
        params={"locale": "en_US", "returnMilestones": "true", "returnSignature": "true"},
        headers=_h(tok), timeout=30)
    if r.status_code != 200: return {"error": f"Tracking {r.status_code}: {r.text[:300]}"}
    d = r.json()
    sh = d.get("trackResponse", {}).get("shipment", [{}])[0]
    pk = sh.get("package", [{}])
    if isinstance(pk, list): pk = pk[0] if pk else {}
    print(f"\n📊 UPS {tn} | weight={pk.get('weight')} | curSt={pk.get('currentStatus',{}).get('description','')}")
    return d

def call_tnt(tok, o_postal, o_country, d_postal, d_country, ship_date=None, weight="1"):
    if not ship_date: ship_date = time.strftime("%Y-%m-%d")
    body = {"originCountryCode": o_country, "originPostalCode": o_postal,
            "destinationCountryCode": d_country, "destinationPostalCode": d_postal,
            "weight": weight, "weightUnitOfMeasure": "KGS", "shipDate": ship_date,
            "shipTime": "10:00:00", "numberOfPackages": "1"}
    r = requests.post(f"{API_BASE}/shipments/v1/transittimes", json=body, headers=_h(tok), timeout=30)
    if r.status_code != 200: return {"error": f"TNT {r.status_code}"}
    return r.json()

def subscribe_track_alert(tok, tn):
    if not WEBHOOK_URL: return {"skipped": True}
    url = f"{API_BASE}/track/v1/subscription/standard/package"
    body = {"trackingNumberList": [tn], "destination": {"url": WEBHOOK_URL, "credentialType": "Bearer", "credential": WEBHOOK_CRED or "default"}, "locale": "en_US"}
    try:
        r = requests.post(url, json=body, headers=_h(tok), timeout=30)
        if r.status_code == 200: print(f"📸 Track Alert abonné: {tn}")
        return r.json() if r.status_code == 200 else {"error": f"TrackAlert {r.status_code}"}
    except Exception as e: return {"error": str(e)}


def track_ups(tn):
    tok = get_token()
    trk = call_tracking(tok, tn)
    if "error" in trk: raise HTTPException(502, trk["error"])
    sh = trk.get("trackResponse", {}).get("shipment", [{}])[0]
    pk = sh.get("package", [{}])
    if isinstance(pk, list): pk = pk[0] if pk else {}
    acts = pk.get("activity", [])
    pkg_addr = pk.get("packageAddress", []); ship_addr = sh.get("shipmentAddress", [])
    orig = safe_get_addr(pkg_addr, ship_addr, acts, "origin")
    dest = safe_get_addr(pkg_addr, ship_addr, acts, "destination")
    tnt = None
    try:
        o_p, o_c = orig.get("postalCode", ""), orig.get("countryCode", "")
        d_p, d_c = dest.get("postalCode", ""), dest.get("countryCode", "")
        pickup = sh.get("pickupDate", "")
        sd = f"{pickup[:4]}-{pickup[4:6]}-{pickup[6:8]}" if pickup and len(pickup) == 8 else None
        w = "1"; alt = pk.get("weight", {})
        if isinstance(alt, dict) and alt.get("weight"): w = alt["weight"]
        if o_p and d_p and o_c and d_c: tnt = call_tnt(tok, o_p, o_c, d_p, d_c, ship_date=sd, weight=w)
    except: tnt = {"error": traceback.format_exc()[:300]}
    svc = detect_service(trk)
    intel = compute_intelligence(trk)
    ta = subscribe_track_alert(tok, tn)
    
    # Extract signature from tracking response
    signature_img = None
    sig_data = pk.get("signature", pk.get("signatureImage", {}))
    if isinstance(sig_data, dict):
        signature_img = sig_data.get("image", sig_data.get("graphicImage", None))
    elif isinstance(sig_data, str) and sig_data:
        signature_img = sig_data
    
    # Also check deliveryInformation for signed-by
    del_info = pk.get("deliveryInformation", {})
    signed_by = ""
    if isinstance(del_info, dict):
        signed_by = del_info.get("receivedBy", del_info.get("signedForByName", ""))
    
    # If delivered but no signature yet, try POD API
    cur_status = pk.get("currentStatus", {})
    if cur_status.get("type") == "D" and not signature_img:
        try:
            pod_r = requests.get(f"{API_BASE}/track/v1/details/{tn}",
                params={"locale": "en_US", "returnSignature": "true"},
                headers=_h(tok), timeout=15)
            if pod_r.status_code == 200:
                pod_d = pod_r.json()
                pod_sh = pod_d.get("trackResponse", {}).get("shipment", [{}])[0]
                pod_pk = pod_sh.get("package", [{}])
                if isinstance(pod_pk, list): pod_pk = pod_pk[0] if pod_pk else {}
                pod_sig = pod_pk.get("signature", {})
                if isinstance(pod_sig, dict) and pod_sig.get("image"):
                    signature_img = pod_sig["image"]
                    print(f"✍️ Signature récupérée via POD pour {tn}")
        except Exception as e:
            print(f"⚠️ POD signature fetch failed: {e}")
    
    # Merge with any webhook-stored proof
    proof = delivery_proofs.get(tn, {})
    if signature_img and not proof.get("signature"):
        proof["signature"] = signature_img
    if signed_by:
        proof["signedBy"] = signed_by
    
    return {"carrier": "ups", "tracking": trk, "timeInTransit": tnt, "serviceDetected": svc,
            "intelligence": intel, "trackAlert": ta, "deliveryProof": proof if proof else None,
            "_meta": {"env": UPS_ENV, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "apis": ["tracking/v1", "transittimes", "track-alert"]}}


# ══════════════════════════════════════════════════════════════
# BPOST
# ══════════════════════════════════════════════════════════════
BPOST_API = "https://track.bpost.cloud/track/items"

def track_bpost(tn, postalCode=""):
    """Track bpost package using public track.bpost.cloud API."""
    print(f"\n📊 bpost {tn} postalCode={postalCode}")
    try:
        headers = {
            "Accept": "application/json",
            "Accept-Language": "fr",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Referer": "https://track.bpost.cloud/btr/web/",
            "Origin": "https://track.bpost.cloud",
        }
        params = {"itemIdentifier": tn}
        if postalCode: params["postalCode"] = postalCode
        
        # Primary: /track/items (the working endpoint)
        r = requests.get("https://track.bpost.cloud/track/items",
                         params=params, headers=headers, timeout=15)
        print(f"📮 bpost response: {r.status_code} len={len(r.text)} body={r.text[:400]}")
        
        if r.status_code != 200 or not r.text.strip().startswith(('[', '{')):
            raise HTTPException(502, f"bpost API: {r.status_code} — vérifiez le code postal")
        
        data = r.json()
        # Response: { items: [...], ... } or { error: "..." }
        if isinstance(data, dict) and data.get("error"):
            raise HTTPException(404, f"bpost: {data['error']}")
        
        items = data.get("items", []) if isinstance(data, dict) else data
        if not items:
            raise HTTPException(404, f"bpost: colis {tn} non trouvé (code postal requis)")
        item = items[0] if isinstance(items, list) else items
    except HTTPException: raise
    except Exception as e:
        print(f"❌ bpost error: {e}")
        raise HTTPException(502, f"bpost: {str(e)}")

    # Normalize to unified format — bpost events have: { date, time, key: { FR: { description }, EN: {...} } }
    events = item.get("events", [])
    active_step = item.get("activeStep", {}) or {}
    sender = item.get("senderCommercialName") or (item.get("sender") or {}).get("name", "")
    step_name = active_step.get("name", "") if isinstance(active_step, dict) else ""
    
    status_map = {"delivered": "D", "in_transit": "I", "transit": "I", "collected": "P",
                  "picked_up": "P", "out_for_delivery": "I", "out_for_delivery_bycar": "I",
                  "announced": "P", "prepare": "P", "processing": "I",
                  "available_at_pickup_point": "I", "ready_for_pickup": "I",
                  "returned": "X", "exception": "X", "refused": "X", "not_delivered": "X"}
    
    activities = []
    for ev in events:
        ev_date = ev.get("date", "")
        ev_time = ev.get("time", "")
        key_data = ev.get("key", {})
        ev_desc = ""
        if isinstance(key_data, dict):
            lang_data = key_data.get("FR") or key_data.get("EN") or key_data.get("NL") or {}
            if isinstance(lang_data, dict): ev_desc = lang_data.get("description", "")
            elif isinstance(lang_data, str): ev_desc = lang_data
        elif isinstance(key_data, str): ev_desc = key_data
        
        ev_loc = ev.get("location", {}) or {}
        
        # Parse date
        parsed_date = ""
        if ev_date:
            d = ev_date.replace("/", "-").replace(".", "-")
            if len(d) == 10 and d[4] == "-": parsed_date = d.replace("-", "")
            elif len(d) == 10 and d[2] == "-": parsed_date = d[6:10] + d[3:5] + d[0:2]
            else: parsed_date = d.replace("-", "")[:8]
        
        parsed_time = ""
        if ev_time:
            t = ev_time.replace(":", "")
            if len(t) == 4: t += "00"
            parsed_time = t[:6]
        
        # Determine type from description
        ev_st = "I"
        dl = ev_desc.lower()
        if any(w in dl for w in ["livré", "delivered", "bezorgd", "afgeleverd"]): ev_st = "D"
        elif any(w in dl for w in ["collecté", "ramassé", "collected", "picked", "opgehaald", "aangemeld", "expédi"]): ev_st = "P"
        elif any(w in dl for w in ["exception", "refusé", "refused", "retour", "returned", "niet bezorgd"]): ev_st = "X"
        
        activities.append({
            "date": parsed_date, "time": parsed_time,
            "status": {"type": ev_st, "description": ev_desc or "—", "code": ""},
            "location": {"address": {
                "city": (ev_loc.get("locationName") or ev_loc.get("name", "")) if isinstance(ev_loc, dict) else str(ev_loc),
                "countryCode": ev_loc.get("countryCode", "BE") if isinstance(ev_loc, dict) else "BE"
            }}
        })

    cur_type = status_map.get(step_name.lower().replace(" ", "_"), "I")
    cur_desc = activities[0]["status"]["description"] if activities else step_name
    if not cur_desc or cur_desc == "—":
        step_lbl = active_step.get("label", {}) if isinstance(active_step, dict) else {}
        if isinstance(step_lbl, dict): cur_desc = step_lbl.get("FR") or step_lbl.get("EN") or step_lbl.get("NL") or step_name
        elif isinstance(step_lbl, str): cur_desc = step_lbl

    exp_range = item.get("expectedDeliveryTimeRange", {}) or {}
    exp_date = item.get("expectedDeliveryDate", "") or item.get("deliveryDate", "") or ""
    del_date = ""
    if exp_date: del_date = exp_date.replace("-", "").replace("/", "")[:8]
    
    # Weight
    weight_g = item.get("weightInGrams", 0)
    weight_obj = {"weight": str(round(weight_g / 1000, 2)) if weight_g else "", "unitOfMeasurement": "KGS"} if weight_g else {}
    
    # Product name
    product_name = item.get("productName", "bpost")
    
    # Addresses
    addresses = []
    snd = item.get("sender", {}) or {}
    rcv = item.get("receiver", item.get("announcedReceiver", {})) or {}
    if snd:
        addresses.append({"type": "ORIGIN", "address": {
            "addressLine": [snd.get("street", "")], "city": snd.get("municipality", ""),
            "postalCode": snd.get("postcode", ""), "countryCode": snd.get("countryCode", "BE")}})
    if rcv:
        addresses.append({"type": "DESTINATION", "address": {
            "addressLine": [f"{rcv.get('streetName', '')} {rcv.get('streetNumber', '')}".strip()],
            "city": rcv.get("municipality", ""), "postalCode": rcv.get("postcode", ""),
            "countryCode": rcv.get("countryCode", "BE")}})
    
    # Process steps → milestones
    milestones = []
    proc_steps = (item.get("processOverview", {}) or {}).get("processSteps", [])
    for ps in proc_steps:
        lbl_main = ps.get("label", {}).get("main", "")
        if isinstance(lbl_main, dict): lbl_main = lbl_main.get("FR") or lbl_main.get("EN") or ""
        milestones.append({
            "description": lbl_main,
            "category": ps.get("name", ""),
            "current": ps.get("status") == "active"
        })
    
    # Order date as pickup
    order_date = item.get("orderDate", {}) or {}
    pickup_date = (order_date.get("day", "") or "").replace("-", "")
    
    normalized = {
        "trackResponse": {
            "shipment": [{
                "service": {"description": f"{product_name}{' — ' + sender if sender else ''}"},
                "pickupDate": pickup_date,
                "package": [{
                    "trackingNumber": tn,
                    "currentStatus": {"type": cur_type, "description": cur_desc},
                    "weight": weight_obj,
                    "dimension": {},
                    "activity": activities,
                    "deliveryDate": [{"type": "RDD", "date": del_date}] if del_date else [],
                    "deliveryTime": {"startTime": exp_range.get("time1", ""), "endTime": exp_range.get("time2", "")} if exp_range else {},
                    "milestones": milestones,
                    "packageAddress": addresses,
                    "deliveryInformation": {},
                }]
            }]
        }
    }

    intel = compute_intelligence(normalized)
    return {"carrier": "bpost", "tracking": normalized, "timeInTransit": None,
            "serviceDetected": {"code": "bpost", "description": f"{product_name}{' — ' + sender if sender else ''}"},
            "intelligence": intel, "trackAlert": {"skipped": True},
            "_meta": {"env": "prod", "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "apis": ["bpost/track"],
                      "raw_bpost": item}}


# ══════════════════════════════════════════════════════════════
# POSTNL
# ══════════════════════════════════════════════════════════════
POSTNL_API_KEY = os.environ.get("POSTNL_API_KEY", "").strip()

def track_postnl(tn, postalCode=""):
    """Track PostNL package using official ShippingStatus API."""
    print(f"\n📊 PostNL {tn} postalCode={postalCode}")
    if not POSTNL_API_KEY:
        raise HTTPException(500, "PostNL API key manquante (POSTNL_API_KEY)")
    try:
        url = f"https://api.postnl.nl/shipment/v2/status/barcode/{tn}"
        params = {"detail": "true"}
        headers = {
            "apikey": POSTNL_API_KEY,
            "Accept": "application/json",
        }
        r = requests.get(url, params=params, headers=headers, timeout=15)
        print(f"🟠 PostNL response: {r.status_code} body={r.text[:400]}")
        
        if r.status_code == 404:
            raise HTTPException(404, f"PostNL: colis {tn} non trouvé")
        if r.status_code == 401:
            raise HTTPException(502, "PostNL: clé API invalide")
        if r.status_code != 200:
            raise HTTPException(502, f"PostNL API: {r.status_code}")
        data = r.json()
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(502, f"PostNL: {str(e)}")

    # Parse PostNL ShippingStatus response
    shipment = {}
    current_status = data.get("CurrentStatus", {})
    if current_status:
        shipment = current_status.get("Shipment", {})
    
    # Complete status has all events
    complete = data.get("CompleteStatus", {})
    if complete:
        shipment = complete.get("Shipment", shipment)
    
    events = shipment.get("Event", [])
    if not isinstance(events, list): events = [events] if events else []
    
    status_obj = shipment.get("Status", {})
    if not isinstance(status_obj, dict): status_obj = {}
    
    # PostNL status code mapping
    def map_status(code, desc=""):
        code = str(code)
        if code in ("7", "11", "12", "13"): return "D"  # Delivered variants
        if code in ("1", "2", "3", "4", "5", "6", "8", "9"): return "I"  # In transit
        if code in ("14", "15", "16", "17", "99"): return "X"  # Exception/returned
        d = (desc or "").lower()
        if "deliver" in d or "bezorg" in d or "uitgereikt" in d: return "D"
        if "exception" in d or "niet" in d or "return" in d: return "X"
        return "I"

    activities = []
    for ev in events:
        dt_str = ev.get("TimeStamp", "")  # format: dd/MM/yyyy HH:mm:ss or ISO
        date_val = ""; time_val = ""
        if "T" in dt_str:
            date_val = dt_str[:10].replace("-", "")
            time_val = dt_str[11:19].replace(":", "")
        elif "/" in dt_str:
            parts = dt_str.split(" ")
            if parts:
                dp = parts[0].split("/")
                if len(dp) == 3: date_val = dp[2] + dp[1] + dp[0]
                if len(parts) > 1: time_val = parts[1].replace(":", "")
        
        code = ev.get("Code", "")
        desc = ev.get("Description", "")
        loc = ev.get("LocationCode", "")
        
        activities.append({
            "date": date_val,
            "time": time_val,
            "status": {
                "type": map_status(code, desc),
                "description": desc,
                "code": code,
            },
            "location": {
                "address": {
                    "city": loc,
                    "countryCode": "NL",
                }
            }
        })

    cur_type = "I"; cur_desc = ""
    cur_code = status_obj.get("StatusCode", "")
    cur_desc = status_obj.get("StatusDescription", "")
    if cur_code: cur_type = map_status(cur_code, cur_desc)
    elif activities:
        cur_type = activities[0]["status"]["type"]
        cur_desc = activities[0]["status"]["description"]

    # Expected delivery
    exp_del = shipment.get("ExpectedDeliveryDate", "")
    del_date = ""
    if exp_del:
        if "T" in exp_del: del_date = exp_del[:10].replace("-", "")
        elif "/" in exp_del:
            dp = exp_del.split("/")
            if len(dp) == 3: del_date = dp[2] + dp[1] + dp[0]

    # Addresses
    addresses = []
    sender = shipment.get("Sender", {})
    receiver = shipment.get("Receiver", {})
    if sender:
        addresses.append({"type": "ORIGIN", "address": {
            "city": sender.get("City", ""), "postalCode": sender.get("Zipcode", ""),
            "countryCode": sender.get("CountryCode", "NL")}})
    if receiver:
        addresses.append({"type": "DESTINATION", "address": {
            "city": receiver.get("City", ""), "postalCode": receiver.get("Zipcode", postalCode),
            "countryCode": receiver.get("CountryCode", "NL")}})

    # Product description
    product = shipment.get("ProductDescription", "PostNL")

    normalized = {
        "trackResponse": {
            "shipment": [{
                "service": {"description": product},
                "pickupDate": "",
                "package": [{
                    "trackingNumber": tn,
                    "currentStatus": {"type": cur_type, "description": cur_desc},
                    "weight": {},
                    "dimension": {},
                    "activity": activities,
                    "deliveryDate": [{"type": "RDD", "date": del_date}] if del_date else [],
                    "deliveryTime": {},
                    "milestones": [],
                    "packageAddress": addresses,
                    "deliveryInformation": {},
                }]
            }]
        }
    }

    intel = compute_intelligence(normalized)
    return {"carrier": "postnl", "tracking": normalized, "timeInTransit": None,
            "serviceDetected": {"code": "postnl", "description": product},
            "intelligence": intel, "trackAlert": {"skipped": True},
            "_meta": {"env": "prod", "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "apis": ["postnl/shippingstatus/v2"]}}


# ══════════════════════════════════════════════════════════════
# NOTIFICATIONS SYSTEM
# ══════════════════════════════════════════════════════════════
notifications = deque(maxlen=200)
sse_clients = []

def send_telegram(text):
    """Envoie un message Telegram."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        print(f"⚠️ Telegram skipped: token={'YES' if TELEGRAM_TOKEN else 'NO'} chat={'YES' if TELEGRAM_CHAT else 'NO'}")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT, "text": text, "parse_mode": "HTML"}
        print(f"📤 Telegram sending to chat {TELEGRAM_CHAT}...")
        r = requests.post(url, json=payload, timeout=10)
        print(f"📤 Telegram response: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"⚠️ Telegram error: {e}")

def add_notification(notif):
    notif["id"] = str(uuid.uuid4())[:8]
    notif["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    notif["read"] = False
    notifications.appendleft(notif)
    data = json.dumps(notif)
    dead = []
    for q in sse_clients:
        try: q.put_nowait(data)
        except: dead.append(q)
    for q in dead: sse_clients.remove(q)
    icon = notif.get("icon", "📦"); title = notif.get("title", ""); body = notif.get("body", "")
    tn = notif.get("tracking", ""); priority = notif.get("priority", "normal")
    tg_msg = f"{icon} <b>{title}</b>\n{body}"
    if tn: tg_msg += f"\n📦 {tn}"
    if priority == "high": tg_msg += "\n🚨 HAUTE PRIORITÉ"
    send_telegram(tg_msg)
    print(f"🔔 Notif: [{notif.get('type','')}] {title} → {len(sse_clients)} SSE + Telegram")

@app.get("/api/notifications/stream")
async def notification_stream():
    q = asyncio.Queue(); sse_clients.append(q)
    async def gen():
        try:
            for n in list(notifications)[:10]: yield f"data: {json.dumps(n)}\n\n"
            while True:
                data = await asyncio.wait_for(q.get(), timeout=30)
                yield f"data: {data}\n\n"
        except asyncio.TimeoutError: yield f"data: {json.dumps({'type':'ping'})}\n\n"
        except asyncio.CancelledError: pass
        finally:
            if q in sse_clients: sse_clients.remove(q)
    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/api/notifications")
def get_notifications(): return list(notifications)

@app.post("/api/notifications/read")
def mark_read():
    for n in notifications: n["read"] = True
    return {"ok": True}

@app.post("/webhook")
async def webhook_handler(request: Request):
    try: body = await request.json()
    except: body = {}
    print(f"\n📨 WEBHOOK reçu: {json.dumps(body, indent=2)[:500]}")
    tn = body.get("trackingNumber", "?"); status_desc = body.get("activityStatusDescription", "")
    status_type = body.get("activityStatusType", ""); city = body.get("activityLocationCity", "")
    country = body.get("activityLocationCountry", ""); actual_time = body.get("actualDeliveryTime", "")
    photo_b64 = body.get("deliveryPhoto", ""); where = f"{city}, {country}" if city else ""
    signature_b64 = body.get("signature", body.get("signatureImage", ""))
    
    # Store delivery proof persistently
    if photo_b64 or signature_b64:
        delivery_proofs[tn] = {
            "photo": photo_b64 or None,
            "signature": signature_b64 or None,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "location": where,
            "time": actual_time,
        }
        print(f"📸 Proof stored for {tn}: photo={'YES' if photo_b64 else 'NO'} sig={'YES' if signature_b64 else 'NO'}")
    
    if status_type == "D":
        add_notification({"type": "delivery", "icon": "✅", "title": f"Colis livré — {tn}",
            "body": f"Livré à {where}" + (f" à {actual_time[:2]}:{actual_time[2:4]}" if len(actual_time) >= 4 else ""),
            "tracking": tn, "priority": "high", "photo": bool(photo_b64), "hasProof": True})
    elif status_type == "X":
        add_notification({"type": "exception", "icon": "⚠️", "title": f"Exception — {tn}",
            "body": f"{status_desc} · {where}", "tracking": tn, "priority": "high"})
    else:
        add_notification({"type": "movement", "icon": "📍", "title": f"Mouvement — {tn}",
            "body": f"{status_desc} · {where}", "tracking": tn, "priority": "normal"})
    return {"status": "ok"}

# Delivery proof storage (in-memory, persists while server runs)
delivery_proofs = {}

@app.get("/api/proof/{tracking_number}")
def get_delivery_proof(tracking_number: str):
    """Get delivery proof (photo + signature) for a tracking number."""
    proof = delivery_proofs.get(tracking_number, {})
    return {"trackingNumber": tracking_number, "proof": proof, "hasProof": bool(proof)}


# ══════════════════════════════════════════════════════════════
# INTELLIGENCE (works with normalized format)
# ══════════════════════════════════════════════════════════════
def compute_intelligence(tracking_data):
    sh = tracking_data.get("trackResponse", {}).get("shipment", [{}])[0]
    pk = sh.get("package", [{}])
    if isinstance(pk, list): pk = pk[0] if pk else {}
    acts = pk.get("activity", []); dd_arr = pk.get("deliveryDate", [])
    dt = pk.get("deliveryTime", {}); cur = pk.get("currentStatus", {})
    miles = pk.get("milestones", []); pickup = sh.get("pickupDate", "")
    now = datetime.utcnow()
    intel = {"delayProbability": None, "delayFactors": [], "transitDays": None,
             "countriesCrossed": [], "facilitiesVisited": [], "nightScans": 0, "weekendScans": 0,
             "avgTimeBetweenScans": None, "lastScanAge": None, "progressPercent": None}
    if not acts: return intel

    def pdt(d, t):
        try:
            ds = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            ts = f"{t[:2]}:{t[2:4]}:{t[4:6]}" if t and len(t) >= 6 and ":" not in t else (t or "00:00:00")
            return datetime.fromisoformat(f"{ds}T{ts}")
        except: return None

    sched = None; rdd = dd_arr[0] if dd_arr else {}
    if rdd.get("date"): sched = pdt(rdd["date"], dt.get("endTime", "235959"))

    act_times, countries, facilities = [], set(), []
    night_count = weekend_count = 0
    for a in acts:
        ts = pdt(a.get("date", ""), a.get("time", ""))
        if ts: act_times.append(ts)
        loc = a.get("location", {}).get("address", {})
        cc = loc.get("countryCode", "")
        if cc: countries.add(cc)
        city = loc.get("city", ""); slic = a.get("location", {}).get("slic", "")
        if city or slic:
            fac = f"{city}, {cc}" if city else f"Centre {slic}"
            if fac not in facilities: facilities.append(fac)
        if ts:
            if ts.hour < 6 or ts.hour >= 22: night_count += 1
            if ts.weekday() >= 5: weekend_count += 1

    intel["countriesCrossed"] = sorted(countries)
    intel["facilitiesVisited"] = facilities
    intel["nightScans"] = night_count; intel["weekendScans"] = weekend_count
    if pickup and len(pickup) == 8:
        pd2 = pdt(pickup, "000000")
        if pd2: intel["transitDays"] = (now - pd2).days
    if len(act_times) >= 2:
        st = sorted(act_times)
        gaps = [(st[i+1]-st[i]).total_seconds()/3600 for i in range(len(st)-1)]
        intel["avgTimeBetweenScans"] = round(sum(gaps)/len(gaps), 1)
    if act_times: intel["lastScanAge"] = round((now - max(act_times)).total_seconds()/3600, 1)
    if miles:
        ci = next((i for i, m in enumerate(miles) if m.get("current")), -1)
        if ci >= 0: intel["progressPercent"] = round((ci/(len(miles)-1))*100) if len(miles) > 1 else 100

    # Check if delivered: currentStatus.type or first activity status
    is_delivered = (cur.get("type", "").upper() == "D" or 
                    (acts and (acts[0].get("status", {}).get("type", "").upper() == "D")))
    
    if is_delivered:
        intel["delayProbability"] = 0; intel["delayFactors"] = ["Colis livré"]
    elif sched:
        score = 0; factors = []
        hours_until = (sched - now).total_seconds() / 3600
        if hours_until < 0: score += 50; factors.append(f"Date dépassée de {abs(round(hours_until))}h")
        elif hours_until < 6: score += 25; factors.append(f"{round(hours_until)}h restantes")
        elif hours_until < 12: score += 10; factors.append(f"{round(hours_until)}h restantes")
        if intel["lastScanAge"] and intel["lastScanAge"] > 24: score += 20; factors.append(f"Dernier scan: {round(intel['lastScanAge'])}h")
        elif intel["lastScanAge"] and intel["lastScanAge"] > 12: score += 10; factors.append(f"Dernier scan: {round(intel['lastScanAge'])}h")
        if intel["progressPercent"] is not None and intel["progressPercent"] < 50 and hours_until < 24:
            score += 15; factors.append(f"Progression {intel['progressPercent']}% avec <24h")
        if cur.get("type") == "X": score += 40; factors.append("Exception")
        if len(countries) > 1: score += 5; factors.append(f"International ({len(countries)} pays)")
        if now.weekday() >= 5 and hours_until < 48: score += 10; factors.append("Weekend")
        intel["delayProbability"] = min(score, 100); intel["delayFactors"] = factors or ["OK"]
        if intel["delayProbability"] >= 50:
            add_notification({"type": "delay_alert", "icon": "🚨",
                "title": f"Risque de retard {intel['delayProbability']}%",
                "body": " · ".join(factors), "tracking": pk.get("trackingNumber", ""), "priority": "high"})
    else:
        intel["delayProbability"] = None; intel["delayFactors"] = ["Pas de date estimée"]
    return intel


def safe_get_addr(pkg_addresses, ship_addrs, activities, role):
    target = ("01", "ORIGIN", "SHIP_FROM") if role == "origin" else ("02", "DESTINATION", "SHIP_TO")
    for pa in (pkg_addresses or []):
        if not isinstance(pa, dict): continue
        pt = pa.get("type", ""); code = pt.get("code", "") if isinstance(pt, dict) else str(pt)
        if code in target: return pa.get("address", {})
    for sa in (ship_addrs or []):
        if not isinstance(sa, dict): continue
        st2 = sa.get("type", ""); code = st2.get("code", "") if isinstance(st2, dict) else str(st2)
        if code in target: return sa.get("address", {})
    if activities:
        idx = -1 if role == "origin" else 0
        return activities[idx].get("location", {}).get("address", {})
    return {}


# ══════════════════════════════════════════════════════════════
# MAIN ENDPOINTS
# ══════════════════════════════════════════════════════════════
@app.get("/api/track/{tracking_number}")
def track(tracking_number: str, carrier: str = Query(default=""), postalCode: str = Query(default="")):
    """Track a package. Auto-detects carrier or use ?carrier=ups|bpost|postnl&postalCode=1000"""
    c = carrier.lower().strip() if carrier else detect_carrier(tracking_number)
    pc = postalCode.strip()
    print(f"\n🔍 Track {tracking_number} → carrier={c} postalCode={pc}")
    if c == "bpost": return track_bpost(tracking_number, pc)
    if c == "postnl": return track_postnl(tracking_number, pc)
    return track_ups(tracking_number)

@app.get("/api/detect/{tracking_number}")
def detect(tracking_number: str):
    """Detect carrier from tracking number."""
    return {"trackingNumber": tracking_number, "carrier": detect_carrier(tracking_number)}

@app.get("/api/health")
def health():
    return {"status": "ok", "ups_env": UPS_ENV, "has_credentials": bool(CLIENT_ID and CLIENT_SECRET),
            "webhook_configured": bool(WEBHOOK_URL), "telegram": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT),
            "postnl_key": bool(POSTNL_API_KEY),
            "carriers": ["ups", "bpost", "postnl"],
            "active_sse_clients": len(sse_clients), "notifications_count": len(notifications),
            "watchlist_count": len(server_watchlist), "polling": "30min"}


# ══════════════════════════════════════════════════════════════
# SERVER-SIDE WATCHLIST + POLLING (30 min)
# ══════════════════════════════════════════════════════════════
server_watchlist = {}  # {tn: {carrier, postalCode, lastStatus, lastDesc, lastCheck}}

@app.post("/api/watchlist/sync")
async def sync_watchlist(request: Request):
    """Sync watchlist from frontend. Body: {items: [{tn, carrier, postalCode}]}"""
    try:
        body = await request.json()
    except:
        body = {}
    items = body.get("items", [])
    # Add new items, keep existing status info
    current_tns = set()
    for it in items:
        tn = it.get("tn", "").strip()
        if not tn: continue
        current_tns.add(tn)
        if tn not in server_watchlist:
            server_watchlist[tn] = {
                "carrier": it.get("carrier", "ups"),
                "postalCode": it.get("postalCode", ""),
                "lastStatus": it.get("status", ""),
                "lastDesc": it.get("desc", ""),
                "lastCheck": None,
            }
        else:
            # Update carrier/postal if changed
            server_watchlist[tn]["carrier"] = it.get("carrier", server_watchlist[tn]["carrier"])
            server_watchlist[tn]["postalCode"] = it.get("postalCode", server_watchlist[tn].get("postalCode", ""))
    # Remove items no longer in watchlist
    for tn in list(server_watchlist.keys()):
        if tn not in current_tns:
            del server_watchlist[tn]
    print(f"📋 Watchlist synced: {len(server_watchlist)} items")
    return {"ok": True, "count": len(server_watchlist)}

@app.get("/api/watchlist")
def get_watchlist():
    return {"items": server_watchlist}


async def poll_watchlist():
    """Background task: poll all watchlist items every 30 minutes."""
    while True:
        await asyncio.sleep(1800)  # 30 minutes
        if not server_watchlist:
            continue
        print(f"\n⏰ POLLING {len(server_watchlist)} colis...")
        for tn, info in list(server_watchlist.items()):
            carrier = info.get("carrier", "ups")
            pc = info.get("postalCode", "")
            old_status = info.get("lastStatus", "")
            old_desc = info.get("lastDesc", "")
            try:
                if carrier == "bpost":
                    result = track_bpost(tn, pc)
                elif carrier == "postnl":
                    result = track_postnl(tn, pc)
                else:
                    result = track_ups(tn)
                
                # Extract new status
                trk = result.get("tracking", {})
                sh = trk.get("trackResponse", {}).get("shipment", [{}])[0]
                pk = sh.get("package", [{}])
                if isinstance(pk, list): pk = pk[0] if pk else {}
                cur = pk.get("currentStatus", {})
                new_status = cur.get("type", "")
                new_desc = cur.get("description", "")
                
                info["lastCheck"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
                
                # Check if status changed
                if new_status and new_status != old_status:
                    info["lastStatus"] = new_status
                    info["lastDesc"] = new_desc
                    
                    carrier_label = {"ups": "UPS", "bpost": "bpost", "postnl": "PostNL"}.get(carrier, carrier)
                    
                    if new_status == "D":
                        add_notification({
                            "type": "delivery", "icon": "✅",
                            "title": f"Colis livré — {carrier_label}",
                            "body": f"{tn}\n{new_desc}",
                            "tracking": tn, "priority": "high"
                        })
                    elif new_status == "X":
                        add_notification({
                            "type": "exception", "icon": "⚠️",
                            "title": f"Exception — {carrier_label}",
                            "body": f"{tn}\n{new_desc}",
                            "tracking": tn, "priority": "high"
                        })
                    else:
                        add_notification({
                            "type": "movement", "icon": "📍",
                            "title": f"Mouvement — {carrier_label}",
                            "body": f"{tn}\n{new_desc}",
                            "tracking": tn, "priority": "normal"
                        })
                    print(f"🔔 Status change: {tn} ({carrier}) {old_status}→{new_status}: {new_desc}")
                elif new_desc and new_desc != old_desc and old_desc:
                    # Description changed but not status type (new scan event)
                    info["lastDesc"] = new_desc
                    carrier_label = {"ups": "UPS", "bpost": "bpost", "postnl": "PostNL"}.get(carrier, carrier)
                    add_notification({
                        "type": "movement", "icon": "📍",
                        "title": f"Mise à jour — {carrier_label}",
                        "body": f"{tn}\n{new_desc}",
                        "tracking": tn, "priority": "normal"
                    })
                    print(f"📍 Desc change: {tn} ({carrier}): {new_desc}")
                else:
                    print(f"   ✓ {tn} ({carrier}): pas de changement")
                    
            except Exception as e:
                print(f"   ⚠️ Poll error {tn} ({carrier}): {e}")
            
            # Small delay between requests to avoid rate limiting
            await asyncio.sleep(2)
        
        print(f"⏰ Polling terminé\n")


@app.on_event("startup")
async def startup_event():
    """Start background polling task."""
    asyncio.create_task(poll_watchlist())
    print("🔄 Background polling started (every 30 min)")

FRONTEND = Path(__file__).parent / "tracker.html"
@app.get("/", response_class=HTMLResponse)
def serve():
    if FRONTEND.exists(): return FRONTEND.read_text(encoding="utf-8")
    return HTMLResponse("<h1>tracker.html manquant</h1>")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    print("=" * 60)
    print("🚀 RAYHANN_EXPLORER CORREOS — UPS + bpost + PostNL + Telegram")
    print("=" * 60)
    print(f"   UPS       : {'✅' if CLIENT_ID else '❌'} ({UPS_ENV.upper()})")
    print(f"   bpost     : ✅ (public API)")
    print(f"   PostNL    : {'✅ key=' + POSTNL_API_KEY[:8] + '...' if POSTNL_API_KEY else '❌ POSTNL_API_KEY manquante'}")
    print(f"   Webhook   : {'✅' if WEBHOOK_URL else '❌'}")
    print(f"   Telegram  : {'✅ Chat ' + TELEGRAM_CHAT if TELEGRAM_TOKEN else '❌'}")
    print(f"   Port      : {port}")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=port)

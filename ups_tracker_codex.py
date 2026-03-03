"""
Multi-Carrier Tracker - version Codex 2026
------------------------------------------

Ce script fournit un outil complet de suivi multi-transporteur. Il prend
en charge les transporteurs suivants :

* **UPS** -- Authentification OAuth, API Track v1, Track Alert (webhooks),
  preuves de livraison (signature, photo, POD).
* **Chronopost** -- API SOAP publique (trackSkybill), sans authentification.
* **PostNL** -- API REST v2 avec cle API.
* **DPD** -- API REST publique (tracking.dpd.de).

Fonctionnalites communes :

* Enregistrement de toutes les reponses brutes dans une base SQLite locale,
  avec extraction et normalisation des evenements individuels.
* Detection automatique du transporteur a partir du format du numero de suivi.
* Affichage d'une timeline lisible pour visualiser l'historique d'un colis.

Utilisation :

   # UPS
   python ups_tracker_codex.py fetch 1ZXXXXXXXXXXXXXXX --milestones --pod --sig --photo

   # Chronopost
   python ups_tracker_codex.py fetch XXXXXXXXXXX --carrier chronopost

   # PostNL
   python ups_tracker_codex.py fetch 3SXXXXXXXXXXXX --carrier postnl

   # DPD
   python ups_tracker_codex.py fetch 01234567890123 --carrier dpd

   # Timeline (detection automatique du transporteur)
   python ups_tracker_codex.py timeline XXXXXXXXXXX --limit 50

   # Track Alert UPS (webhooks)
   python ups_tracker_codex.py subscribe 1ZXXX1 1ZXXX2
"""

import os
import re
import uuid
import json
import time
import base64
import sqlite3
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv


# Chargement des variables d'environnement depuis `.env` si present.
load_dotenv()

# ---------------------------------------------------------------------------
# Transporteurs supportes
# ---------------------------------------------------------------------------
CARRIERS = ("ups", "chronopost", "postnl", "dpd")


def _db() -> sqlite3.Connection:
    """Retourne un connecteur SQLite et cree les tables necessaires si elles n'existent pas."""
    db_path = os.environ.get("UPS_DB", "ups_tracking.sqlite3")
    con = sqlite3.connect(db_path)
    # Table des reponses brutes (historique complet des appels API)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS track_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracking_number TEXT NOT NULL,
            fetched_at_utc INTEGER NOT NULL,
            source TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            carrier TEXT NOT NULL DEFAULT 'ups'
        )
        """
    )
    # Table des evenements individuels normalises
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS track_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracking_number TEXT NOT NULL,
            event_ts TEXT,
            event_city TEXT,
            event_state TEXT,
            event_postal TEXT,
            event_country TEXT,
            status_type TEXT,
            status_code TEXT,
            status_desc TEXT,
            raw_json TEXT NOT NULL,
            carrier TEXT NOT NULL DEFAULT 'ups',
            UNIQUE(
                tracking_number,
                event_ts,
                event_city,
                event_state,
                event_postal,
                event_country,
                status_type,
                status_code
            )
        )
        """
    )
    # Table des abonnements Track Alert crees via l'API
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracking_number TEXT NOT NULL UNIQUE,
            created_at_utc INTEGER NOT NULL,
            env TEXT NOT NULL
        )
        """
    )
    # Table des preuves de livraison (signature, photo)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_proofs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracking_number TEXT NOT NULL,
            proof_type TEXT NOT NULL,
            image_data TEXT NOT NULL,
            image_format TEXT,
            captured_at_utc INTEGER NOT NULL,
            UNIQUE(tracking_number, proof_type)
        )
        """
    )
    # Migration : ajouter la colonne carrier si elle n'existe pas encore
    for table in ("track_raw", "track_events"):
        try:
            con.execute(f"ALTER TABLE {table} ADD COLUMN carrier TEXT NOT NULL DEFAULT 'ups'")
        except sqlite3.OperationalError:
            pass  # colonne deja presente
    con.commit()
    return con


def detect_carrier(tracking_number: str) -> str:
    """Detecte automatiquement le transporteur a partir du format du numero de suivi.

    Heuristiques :
    - UPS : commence par '1Z' suivi de caracteres alphanumeriques (18 car.)
    - Chronopost : 13 a 15 caracteres alphanumeriques, souvent 2 lettres + chiffres + 2 lettres
    - PostNL : commence par '3S' suivi de caracteres alphanumeriques
    - DPD : 14 chiffres purs ou commence par '0' suivi de 13 chiffres

    En cas de doute, retourne 'ups' par defaut.
    """
    tn = tracking_number.strip().upper()
    # UPS : 1Z...
    if tn.startswith("1Z"):
        return "ups"
    # PostNL : 3S...
    if tn.startswith("3S"):
        return "postnl"
    # DPD : 14 chiffres
    if re.fullmatch(r"\d{14}", tn):
        return "dpd"
    # Chronopost : 13-15 caracteres alphanumeriques (format typique : XX000000000XX)
    if re.fullmatch(r"[A-Z0-9]{11,15}", tn) and not tn.startswith("1Z"):
        return "chronopost"
    return "ups"


# =====================================================================
# UPS
# =====================================================================

@dataclass
class UpsConfig:
    """Structure de configuration pour acceder a l'API UPS."""

    client_id: str
    client_secret: str
    env: str  # "prod" ou "cie"

    @property
    def api_base(self) -> str:
        return (
            "https://onlinetools.ups.com/api"
            if self.env == "prod"
            else "https://wwwcie.ups.com/api"
        )

    @property
    def oauth_token_url(self) -> str:
        host = (
            "https://onlinetools.ups.com"
            if self.env == "prod"
            else "https://wwwcie.ups.com"
        )
        return f"{host}/security/v1/oauth/token"

    @property
    def track_alert_base(self) -> str:
        host = (
            "https://onlinetools.ups.com"
            if self.env == "prod"
            else "https://wwwcie.ups.com"
        )
        return f"{host}/api/track/v1"


class UpsAuth:
    """Gestion d'authentification OAuth UPS."""

    def __init__(self, cfg: UpsConfig):
        self.cfg = cfg
        self._token: Optional[Tuple[str, float]] = None

    def get_token(self) -> str:
        if self._token and time.time() < self._token[1] - 30:
            return self._token[0]
        basic = base64.b64encode(f"{self.cfg.client_id}:{self.cfg.client_secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "client_credentials"}
        resp = requests.post(self.cfg.oauth_token_url, headers=headers, data=data, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"OAuth failed {resp.status_code}: {resp.text}")
        payload = resp.json()
        token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._token = (token, time.time() + expires_in)
        return token


class UpsTracker:
    """Client de haut niveau pour consommer l'API UPS Tracking et Track Alert."""

    CARRIER = "ups"

    def __init__(self, cfg: UpsConfig):
        self.cfg = cfg
        self.auth = UpsAuth(cfg)

    @staticmethod
    def _trans_headers() -> Dict[str, str]:
        return {
            "transId": str(uuid.uuid4()),
            "transactionSrc": "codex-ups-tracker",
        }

    def fetch_tracking_details(
        self,
        tracking_number: str,
        locale: str = "en_US",
        return_signature: bool = False,
        return_pod: bool = False,
        return_milestones: bool = True,
        return_photo: bool = False,
    ) -> Dict[str, Any]:
        token = self.auth.get_token()
        url = f"{self.cfg.api_base}/track/v1/details/{tracking_number}"
        params = {
            "locale": locale,
            "returnSignature": str(return_signature).lower(),
            "returnPOD": str(return_pod).lower(),
            "returnMilestones": str(return_milestones).lower(),
            "returnPhoto": str(return_photo).lower(),
        }
        headers = {
            **self._trans_headers(),
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"UPS Track failed {resp.status_code}: {resp.text}")
        payload = resp.json()
        con = _db()
        con.execute(
            "INSERT INTO track_raw(tracking_number, fetched_at_utc, source, payload_json, carrier) VALUES(?,?,?,?,?)",
            (tracking_number, int(time.time()), "track_api", json.dumps(payload), self.CARRIER),
        )
        con.commit()
        self._extract_and_store_events(tracking_number, payload)
        self._extract_and_store_proofs(tracking_number, payload)
        return payload

    def _extract_and_store_events(self, tracking_number: str, payload: Dict[str, Any]) -> None:
        con = _db()
        shipments: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            shipments = (
                payload.get("trackResponse", {}).get("shipment", [])
                or payload.get("shipment", [])
            )
        for sh in shipments:
            packages = sh.get("package", []) if isinstance(sh, dict) else []
            for pkg in packages:
                activities = pkg.get("activity", []) if isinstance(pkg, dict) else []
                for act in activities:
                    date_str = act.get("date") or act.get("gmtDate") or ""
                    time_str = act.get("time") or act.get("gmtTime") or ""
                    event_ts = ""
                    if date_str and len(date_str) == 8 and time_str and len(time_str) >= 6:
                        event_ts = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                    loc = act.get("location", {}) or {}
                    addr = loc.get("address", {}) if isinstance(loc, dict) else {}
                    city = addr.get("city", "")
                    state = addr.get("stateProvince", "")
                    postal = addr.get("postalCode", "")
                    country = addr.get("countryCode", addr.get("country", ""))
                    status = act.get("status", {}) or {}
                    stype = status.get("type", "")
                    scode = status.get("code", "")
                    sdesc = status.get("description", "")
                    try:
                        con.execute(
                            """
                            INSERT OR IGNORE INTO track_events(
                                tracking_number, event_ts,
                                event_city, event_state, event_postal, event_country,
                                status_type, status_code, status_desc,
                                raw_json, carrier
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (tracking_number, event_ts, city, state, postal, country,
                             stype, scode, sdesc, json.dumps(act), self.CARRIER),
                        )
                    except sqlite3.Error:
                        pass
        con.commit()

    def _extract_and_store_proofs(self, tracking_number: str, payload: Dict[str, Any]) -> None:
        con = _db()
        shipments: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            shipments = (
                payload.get("trackResponse", {}).get("shipment", [])
                or payload.get("shipment", [])
            )
        for sh in shipments:
            packages = sh.get("package", []) if isinstance(sh, dict) else []
            for pkg in packages:
                sig = pkg.get("signatureImage") or {}
                if isinstance(sig, dict) and sig.get("image"):
                    try:
                        con.execute(
                            "INSERT OR REPLACE INTO delivery_proofs(tracking_number, proof_type, image_data, image_format, captured_at_utc) VALUES(?,?,?,?,?)",
                            (tracking_number, "signature", sig["image"], sig.get("imageFormat", "GIF"), int(time.time())),
                        )
                    except sqlite3.Error:
                        pass
                photo = pkg.get("deliveryPhoto") or {}
                if isinstance(photo, dict) and (photo.get("photo") or photo.get("image")):
                    img = photo.get("photo") or photo.get("image")
                    try:
                        con.execute(
                            "INSERT OR REPLACE INTO delivery_proofs(tracking_number, proof_type, image_data, image_format, captured_at_utc) VALUES(?,?,?,?,?)",
                            (tracking_number, "photo", img, photo.get("photoFormat", "PNG"), int(time.time())),
                        )
                    except sqlite3.Error:
                        pass
        con.commit()

    def subscribe_track_alert(
        self,
        tracking_numbers: List[str],
        webhook_url: str,
        webhook_credential: str,
        locale: str = "en_US",
        country_code: str = "US",
    ) -> Dict[str, Any]:
        token = self.auth.get_token()
        url = f"{self.cfg.track_alert_base}/subscription/standard/package"
        body = {
            "locale": locale,
            "countryCode": country_code,
            "trackingNumberList": tracking_numbers,
            "destination": {
                "url": webhook_url,
                "credentialType": "Bearer",
                "credential": webhook_credential,
            },
        }
        headers = {
            **self._trans_headers(),
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        resp = requests.post(url, json=body, headers=headers, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Track Alert subscription failed {resp.status_code}: {resp.text}")
        payload = resp.json()
        con = _db()
        for tn in payload.get("validTrackingNumbers", []) or []:
            con.execute(
                "INSERT OR IGNORE INTO subscriptions(tracking_number, created_at_utc, env) VALUES(?,?,?)",
                (tn, int(time.time()), self.cfg.env),
            )
        con.commit()
        return payload


# =====================================================================
# CHRONOPOST
# =====================================================================

class ChronopostTracker:
    """Client de suivi Chronopost via l'API SOAP publique (trackSkybill).

    Aucune authentification n'est requise pour le suivi basique.
    """

    CARRIER = "chronopost"
    SOAP_URL = "https://ws.chronopost.fr/tracking-cxf/TrackingServiceWS"

    def fetch_tracking_details(
        self,
        tracking_number: str,
        locale: str = "fr_FR",
    ) -> Dict[str, Any]:
        """Appelle le service SOAP trackSkybill de Chronopost."""
        soap_body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"'
            ' xmlns:cxf="http://cxf.tracking.soap.chronopost.fr/">'
            "<soapenv:Header/>"
            "<soapenv:Body>"
            "<cxf:trackSkybill>"
            f"<language>{locale}</language>"
            f"<skybillNumber>{tracking_number}</skybillNumber>"
            "</cxf:trackSkybill>"
            "</soapenv:Body>"
            "</soapenv:Envelope>"
        )
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": "",
        }
        resp = requests.post(self.SOAP_URL, data=soap_body.encode("utf-8"), headers=headers, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Chronopost SOAP failed {resp.status_code}: {resp.text}")

        # Parser la reponse SOAP XML
        payload = self._parse_soap_response(resp.text)

        # Stocker la reponse brute
        con = _db()
        con.execute(
            "INSERT INTO track_raw(tracking_number, fetched_at_utc, source, payload_json, carrier) VALUES(?,?,?,?,?)",
            (tracking_number, int(time.time()), "chronopost_soap", json.dumps(payload), self.CARRIER),
        )
        con.commit()

        # Extraire et stocker les evenements
        self._extract_and_store_events(tracking_number, payload)
        return payload

    def _parse_soap_response(self, xml_text: str) -> Dict[str, Any]:
        """Parse la reponse SOAP de Chronopost et retourne un dict Python."""
        # Supprimer les prefixes de namespace pour simplifier le parsing
        cleaned = re.sub(r"<(/?)[\w]+:", r"<\1", xml_text)
        root = ET.fromstring(cleaned)

        result: Dict[str, Any] = {"events": []}

        # Chercher les elements listEvents/events dans la reponse
        for event_elem in root.iter("events"):
            event: Dict[str, str] = {}
            for child in event_elem:
                event[child.tag] = (child.text or "").strip()
            if event:
                result["events"].append(event)

        # Chercher aussi les informations generales du colis
        for return_elem in root.iter("return"):
            for child in return_elem:
                if child.tag not in ("listEvents", "events") and child.text:
                    result[child.tag] = child.text.strip()
            break  # un seul element return

        return result

    def _extract_and_store_events(self, tracking_number: str, payload: Dict[str, Any]) -> None:
        """Extrait les evenements de la reponse Chronopost et les stocke en base."""
        con = _db()
        for evt in payload.get("events", []):
            # eventDate format : "jj/mm/aaaa hh:mm" ou "aaaa-mm-jjThh:mm:ss"
            raw_date = evt.get("eventDate", "")
            event_ts = self._normalize_date(raw_date)

            city = evt.get("officeName", "")
            postal = evt.get("zipCode", "")
            country = evt.get("countryName", "")
            scode = evt.get("code", "")
            sdesc = evt.get("eventLabel", "")

            try:
                con.execute(
                    """
                    INSERT OR IGNORE INTO track_events(
                        tracking_number, event_ts,
                        event_city, event_state, event_postal, event_country,
                        status_type, status_code, status_desc,
                        raw_json, carrier
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (tracking_number, event_ts, city, "", postal, country,
                     "", scode, sdesc, json.dumps(evt), self.CARRIER),
                )
            except sqlite3.Error:
                pass
        con.commit()

    @staticmethod
    def _normalize_date(raw: str) -> str:
        """Normalise les differents formats de date Chronopost en ISO 8601."""
        if not raw:
            return ""
        # Format dd/mm/yyyy HH:MM
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})\s+(\d{2}):(\d{2})", raw)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}T{m.group(4)}:{m.group(5)}:00"
        # Format yyyy-mm-ddTHH:MM:SS (deja ISO)
        if "T" in raw and len(raw) >= 19:
            return raw[:19]
        return raw


# =====================================================================
# POSTNL
# =====================================================================

@dataclass
class PostnlConfig:
    """Configuration pour l'API PostNL."""
    api_key: str


class PostnlTracker:
    """Client de suivi PostNL via l'API REST Barcode Status v2.

    Necessite une cle API obtenue depuis le portail developpeur PostNL.
    """

    CARRIER = "postnl"
    API_BASE = "https://api.postnl.nl/shipment/v2/status"

    def __init__(self, cfg: PostnlConfig):
        self.cfg = cfg

    def fetch_tracking_details(
        self,
        tracking_number: str,
    ) -> Dict[str, Any]:
        """Recupere le statut d'un colis PostNL par son code-barres."""
        url = f"{self.API_BASE}/barcode/{tracking_number}"
        headers = {
            "apikey": self.cfg.api_key,
            "Accept": "application/json",
        }
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"PostNL API failed {resp.status_code}: {resp.text}")
        payload = resp.json()

        # Stocker la reponse brute
        con = _db()
        con.execute(
            "INSERT INTO track_raw(tracking_number, fetched_at_utc, source, payload_json, carrier) VALUES(?,?,?,?,?)",
            (tracking_number, int(time.time()), "postnl_api", json.dumps(payload), self.CARRIER),
        )
        con.commit()

        self._extract_and_store_events(tracking_number, payload)
        return payload

    def _extract_and_store_events(self, tracking_number: str, payload: Dict[str, Any]) -> None:
        """Extrait les evenements de la reponse PostNL et les stocke en base."""
        con = _db()
        # Structure : CurrentStatus.Shipment.Events[]
        shipment = (
            payload.get("CurrentStatus", {}).get("Shipment", {})
            or payload.get("Shipment", {})
        )
        events = shipment.get("Events") or []
        if isinstance(events, dict):
            events = [events]

        for evt in events:
            raw_ts = evt.get("TimeStamp", "")
            event_ts = self._normalize_date(raw_ts)
            scode = evt.get("Code") or evt.get("StatusCode") or ""
            sdesc = evt.get("Description") or evt.get("StatusDescription") or ""
            location_code = evt.get("LocationCode", "")
            dest_code = evt.get("DestinationLocationCode", "")
            city = location_code or dest_code
            country = "NL"

            try:
                con.execute(
                    """
                    INSERT OR IGNORE INTO track_events(
                        tracking_number, event_ts,
                        event_city, event_state, event_postal, event_country,
                        status_type, status_code, status_desc,
                        raw_json, carrier
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (tracking_number, event_ts, city, "", "", country,
                     "", scode, sdesc, json.dumps(evt), self.CARRIER),
                )
            except sqlite3.Error:
                pass

        # Stocker aussi le statut courant comme evenement
        current_status = shipment.get("Status") or {}
        if current_status and current_status.get("TimeStamp"):
            event_ts = self._normalize_date(current_status["TimeStamp"])
            phase = current_status.get("PhaseDescription", "")
            sdesc = current_status.get("StatusDescription", "")
            full_desc = f"{phase} - {sdesc}" if phase and sdesc else (phase or sdesc)
            try:
                con.execute(
                    """
                    INSERT OR IGNORE INTO track_events(
                        tracking_number, event_ts,
                        event_city, event_state, event_postal, event_country,
                        status_type, status_code, status_desc,
                        raw_json, carrier
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (tracking_number, event_ts, "", "", "", "NL",
                     current_status.get("PhaseCode", ""), current_status.get("StatusCode", ""),
                     full_desc, json.dumps(current_status), self.CARRIER),
                )
            except sqlite3.Error:
                pass

        con.commit()

    @staticmethod
    def _normalize_date(raw: str) -> str:
        """Normalise les formats de date PostNL en ISO 8601."""
        if not raw:
            return ""
        # Format dd-mm-yyyy HH:MM:SS
        m = re.match(r"(\d{2})-(\d{2})-(\d{4})\s+(\d{2}):(\d{2}):(\d{2})", raw)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}T{m.group(4)}:{m.group(5)}:{m.group(6)}"
        # Format yyyy-mm-ddTHH:MM:SS
        if "T" in raw and len(raw) >= 19:
            return raw[:19]
        return raw


# =====================================================================
# DPD
# =====================================================================

class DpdTracker:
    """Client de suivi DPD via l'API REST publique de tracking.dpd.de.

    Aucune authentification n'est requise.
    """

    CARRIER = "dpd"
    API_BASE = "https://tracking.dpd.de/rest/plc"

    def fetch_tracking_details(
        self,
        tracking_number: str,
        locale: str = "fr_FR",
    ) -> Dict[str, Any]:
        """Recupere les informations de suivi DPD."""
        url = f"{self.API_BASE}/{locale}/{tracking_number}"
        headers = {"Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"DPD API failed {resp.status_code}: {resp.text}")
        payload = resp.json()

        # Stocker la reponse brute
        con = _db()
        con.execute(
            "INSERT INTO track_raw(tracking_number, fetched_at_utc, source, payload_json, carrier) VALUES(?,?,?,?,?)",
            (tracking_number, int(time.time()), "dpd_api", json.dumps(payload), self.CARRIER),
        )
        con.commit()

        self._extract_and_store_events(tracking_number, payload)
        return payload

    def _extract_and_store_events(self, tracking_number: str, payload: Dict[str, Any]) -> None:
        """Extrait les evenements de la reponse DPD et les stocke en base."""
        con = _db()
        # Structure : parcellifecycleResponse.parcelLifeCycleData.scanInfo.scan[]
        lifecycle = payload.get("parcellifecycleResponse", {})
        lcd = lifecycle.get("parcelLifeCycleData", {})
        if isinstance(lcd, list):
            lcd = lcd[0] if lcd else {}
        scans = lcd.get("scanInfo", {}).get("scan", [])
        if isinstance(scans, dict):
            scans = [scans]

        for scan in scans:
            date_str = scan.get("date", "")
            time_str = scan.get("time", "")
            event_ts = ""
            if date_str and len(date_str) == 8 and time_str and len(time_str) >= 6:
                event_ts = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
            elif date_str and len(date_str) == 10:
                # Format dd/mm/yyyy
                m = re.match(r"(\d{2})/(\d{2})/(\d{4})", date_str)
                if m:
                    ts_time = time_str if time_str else "00:00:00"
                    event_ts = f"{m.group(3)}-{m.group(2)}-{m.group(1)}T{ts_time}"

            scan_type = scan.get("scanType", {}) or {}
            scode = scan_type.get("code", "")
            sdesc = scan_type.get("description", "") or scan.get("scanDescription", "")

            scan_data = scan.get("scanData", {}) or {}
            city = scan_data.get("location", "")
            country = scan_data.get("country", "")

            try:
                con.execute(
                    """
                    INSERT OR IGNORE INTO track_events(
                        tracking_number, event_ts,
                        event_city, event_state, event_postal, event_country,
                        status_type, status_code, status_desc,
                        raw_json, carrier
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (tracking_number, event_ts, city, "", "", country,
                     "", scode, sdesc, json.dumps(scan), self.CARRIER),
                )
            except sqlite3.Error:
                pass
        con.commit()


# =====================================================================
# Timeline partagee (tous transporteurs)
# =====================================================================

def timeline(tracking_number: str, carrier: Optional[str] = None, limit: int = 50) -> List[Tuple]:
    """Retourne une liste d'evenements classes du plus recent au plus ancien."""
    con = _db()
    if carrier:
        cur = con.execute(
            """
            SELECT event_ts, status_type, status_code, status_desc,
                   event_city, event_state, event_postal, event_country, carrier
            FROM track_events
            WHERE tracking_number = ? AND carrier = ?
            ORDER BY event_ts DESC
            LIMIT ?
            """,
            (tracking_number, carrier, limit),
        )
    else:
        cur = con.execute(
            """
            SELECT event_ts, status_type, status_code, status_desc,
                   event_city, event_state, event_postal, event_country, carrier
            FROM track_events
            WHERE tracking_number = ?
            ORDER BY event_ts DESC
            LIMIT ?
            """,
            (tracking_number, limit),
        )
    return cur.fetchall()


# =====================================================================
# CLI
# =====================================================================

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Multi-carrier tracker : UPS, Chronopost, PostNL, DPD"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # --- fetch ---
    p_fetch = sub.add_parser(
        "fetch", help="Recupere les informations de suivi pour un numero"
    )
    p_fetch.add_argument("tracking_number", help="Numero de suivi")
    p_fetch.add_argument(
        "--carrier", choices=CARRIERS, default=None,
        help="Transporteur (auto-detecte si omis)",
    )
    # Options specifiques UPS
    p_fetch.add_argument("--pod", action="store_true", help="[UPS] Inclure la preuve de livraison")
    p_fetch.add_argument("--sig", action="store_true", help="[UPS] Inclure l'image de signature")
    p_fetch.add_argument("--milestones", action="store_true", default=True, help="[UPS] Inclure les jalons")
    p_fetch.add_argument("--photo", action="store_true", help="[UPS] Inclure la photo de livraison")

    # --- timeline ---
    p_tl = sub.add_parser(
        "timeline", help="Affiche les evenements stockes d'un numero de suivi"
    )
    p_tl.add_argument("tracking_number")
    p_tl.add_argument("--carrier", choices=CARRIERS, default=None, help="Filtrer par transporteur")
    p_tl.add_argument("--limit", type=int, default=50, help="Nombre d'evenements a afficher")

    # --- subscribe (UPS uniquement) ---
    p_sub = sub.add_parser(
        "subscribe", help="[UPS] Cree un abonnement Track Alert pour un ou plusieurs numeros"
    )
    p_sub.add_argument("tracking_numbers", nargs="+", help="Un ou plusieurs numeros de suivi")
    p_sub.add_argument(
        "--webhook-url",
        default=os.environ.get("TRACK_ALERT_WEBHOOK_URL", ""),
        help="URL du webhook qui recevra les notifications",
    )
    p_sub.add_argument(
        "--webhook-credential",
        default=os.environ.get("TRACK_ALERT_WEBHOOK_CREDENTIAL", ""),
        help="Jeton partage transmis dans l'en-tete 'credential'",
    )

    args = parser.parse_args()

    # ---- FETCH ----
    if args.cmd == "fetch":
        carrier = args.carrier or detect_carrier(args.tracking_number)
        print(f"Transporteur detecte : {carrier.upper()}")

        if carrier == "ups":
            client_id = os.environ.get("UPS_CLIENT_ID", "").strip()
            client_secret = os.environ.get("UPS_CLIENT_SECRET", "").strip()
            env = os.environ.get("UPS_ENV", "prod").strip().lower() or "prod"
            if env not in ("prod", "cie"):
                env = "prod"
            if not client_id or not client_secret:
                raise SystemExit(
                    "Configurez UPS_CLIENT_ID et UPS_CLIENT_SECRET dans .env"
                )
            cfg = UpsConfig(client_id=client_id, client_secret=client_secret, env=env)
            tracker = UpsTracker(cfg)
            payload = tracker.fetch_tracking_details(
                args.tracking_number,
                return_signature=args.sig,
                return_pod=args.pod,
                return_milestones=args.milestones,
                return_photo=args.photo,
            )

        elif carrier == "chronopost":
            tracker = ChronopostTracker()
            payload = tracker.fetch_tracking_details(args.tracking_number)

        elif carrier == "postnl":
            api_key = os.environ.get("POSTNL_API_KEY", "").strip()
            if not api_key:
                raise SystemExit(
                    "Configurez POSTNL_API_KEY dans .env"
                )
            cfg = PostnlConfig(api_key=api_key)
            tracker = PostnlTracker(cfg)
            payload = tracker.fetch_tracking_details(args.tracking_number)

        elif carrier == "dpd":
            tracker = DpdTracker()
            payload = tracker.fetch_tracking_details(args.tracking_number)

        else:
            raise SystemExit(f"Transporteur inconnu : {carrier}")

        print(json.dumps(payload, indent=2, ensure_ascii=False))
        print(
            f"\nOK. Les evenements ont ete stockes. "
            f"Utilisez 'timeline {args.tracking_number}' pour voir la timeline."
        )

    # ---- TIMELINE ----
    elif args.cmd == "timeline":
        carrier = args.carrier  # None = tous les transporteurs
        rows = timeline(args.tracking_number, carrier=carrier, limit=args.limit)
        if not rows:
            print(
                "Aucun evenement en base pour ce numero. "
                "Lancez d'abord 'fetch' pour recuperer les informations."
            )
            return
        for row in rows:
            (ts, stype, scode, sdesc, city, state, postal, country, c) = row
            where = " / ".join([x for x in [city, state, postal, country] if x])
            carrier_tag = f"[{c.upper()}] " if c else ""
            print(f"- {ts or '??'} | {carrier_tag}{stype or ''}{'-' if stype and scode else ''}{scode or ''} | {sdesc or ''} | {where}")

    # ---- SUBSCRIBE (UPS) ----
    elif args.cmd == "subscribe":
        client_id = os.environ.get("UPS_CLIENT_ID", "").strip()
        client_secret = os.environ.get("UPS_CLIENT_SECRET", "").strip()
        env = os.environ.get("UPS_ENV", "prod").strip().lower() or "prod"
        if env not in ("prod", "cie"):
            env = "prod"
        if not client_id or not client_secret:
            raise SystemExit(
                "Configurez UPS_CLIENT_ID et UPS_CLIENT_SECRET dans .env"
            )
        if not args.webhook_url or not args.webhook_credential:
            raise SystemExit(
                "Specifiez --webhook-url et --webhook-credential ou configurez "
                "TRACK_ALERT_WEBHOOK_URL et TRACK_ALERT_WEBHOOK_CREDENTIAL dans .env"
            )
        cfg = UpsConfig(client_id=client_id, client_secret=client_secret, env=env)
        tracker = UpsTracker(cfg)
        payload = tracker.subscribe_track_alert(
            args.tracking_numbers,
            args.webhook_url,
            args.webhook_credential,
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        print(
            "\nOK. Abonnement cree. Verifiez votre serveur webhook pour les notifications entrantes."
        )


if __name__ == "__main__":
    main()

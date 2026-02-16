"""
UPS Tracker - version Codex 2026
--------------------------------

Ce script fournit un exemple complet de logiciel de suivi UPS que vous pouvez
utiliser et étendre avec Codex. Il prend en charge :

* Authentification OAuth auprès de l'API UPS.
* Récupération détaillée des informations de suivi pour un numéro UPS donné,
  avec option pour inclure les jalons (`milestones`), la preuve de livraison
  (`POD`), l'image de signature et la photo de livraison lorsque ces
  informations sont autorisées par UPS.
* Enregistrement de toutes les réponses brutes dans une base SQLite locale,
  ainsi qu'extraction et normalisation des événements individuels (date,
  localisation, code et description de statut, etc.).
* Affichage d'une « timeline » lisible pour visualiser l'historique d'un
  colis.
* Création d'abonnements Track Alert (webhooks) pour recevoir les mises à
  jour en quasi temps réel via l'API UPS Track Alert. La réception des
  notifications peut être gérée par un serveur FastAPI minimal (voir
  `webhook_server.py` pour un exemple).

Pour utiliser ce programme :

1. Installez les dépendances requises :

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install requests fastapi uvicorn pydantic python-dotenv
   ```

2. Créez un fichier `.env` à la racine du projet avec vos identifiants UPS :

   ```env
   UPS_CLIENT_ID=VotreClientID
   UPS_CLIENT_SECRET=VotreClientSecret
   UPS_ENV=prod  # ou "cie" pour l'environnement de test
   TRACK_ALERT_WEBHOOK_URL=https://votre-domaine.exemple/ups/webhook
   TRACK_ALERT_WEBHOOK_CREDENTIAL=un_secret_partage
   UPS_DB=ups_tracking.sqlite3
   ```

3. Exécutez le script en ligne de commande. Quelques exemples :

   - Récupérer les informations détaillées pour un numéro de suivi :

     ```bash
     python ups_tracker_codex.py fetch 1ZXXXXXXXXXXXXXXX --milestones --pod --sig --photo
     ```

   - Afficher les 50 derniers événements pour ce colis :

     ```bash
     python ups_tracker_codex.py timeline 1ZXXXXXXXXXXXXXXX --limit 50
     ```

   - Créer un abonnement Track Alert pour plusieurs numéros :

     ```bash
     python ups_tracker_codex.py subscribe 1ZXXX1 1ZXXX2
     ```

4. (Optionnel) Lancez le serveur webhook (voir `webhook_server.py`) pour
   enregistrer les notifications et développez votre propre logique (envoi
   d'email, intégration CRM, etc.).

Ce script est volontairement verbeux et documenté. Vous pouvez
l'utiliser tel quel ou comme base pour développer une application plus
complexe (interface web, tableau de bord, intégration ERP, etc.).
"""

import os
import uuid
import json
import time
import base64
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv


# Chargement des variables d'environnement depuis `.env` si présent.
load_dotenv()


def _db() -> sqlite3.Connection:
    """Retourne un connecteur SQLite et crée les tables nécessaires si elles n'existent pas."""
    db_path = os.environ.get("UPS_DB", "ups_tracking.sqlite3")
    con = sqlite3.connect(db_path)
    # Table des réponses brutes (historique complet des appels API)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS track_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracking_number TEXT NOT NULL,
            fetched_at_utc INTEGER NOT NULL,
            source TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    # Table des événements individuels normalisés
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
    # Table des abonnements Track Alert créés via l'API
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
    con.commit()
    return con


@dataclass
class UpsConfig:
    """Structure de configuration pour accéder à l'API UPS."""

    client_id: str
    client_secret: str
    env: str  # "prod" ou "cie"

    @property
    def api_base(self) -> str:
        """Base de l'URL API selon l'environnement."""
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
        # Token et timestamp d'expiration (epoch UTC). Le token est mis en
        # cache tant qu'il est valable.
        self._token: Optional[Tuple[str, float]] = None

    def get_token(self) -> str:
        """Récupère un token d'accès OAuth, en le réutilisant s'il n'est pas expiré."""
        # Réutiliser si token non expiré
        if self._token and time.time() < self._token[1] - 30:
            return self._token[0]

        # Encodage Basic Auth pour client_id:client_secret
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

    def __init__(self, cfg: UpsConfig):
        self.cfg = cfg
        self.auth = UpsAuth(cfg)

    @staticmethod
    def _trans_headers() -> Dict[str, str]:
        """Génère des entêtes de transaction uniques requis par l'API."""
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
        """
        Appelle l'API de suivi UPS pour un numéro donné.

        :param tracking_number: numéro de suivi (1Z...).
        :param locale: langue de réponse, ex. "fr_CH" ou "en_US".
        :param return_signature: booléen demandant l'image de signature (si autorisée).
        :param return_pod: booléen demandant la preuve de livraison PDF (si autorisée).
        :param return_milestones: booléen demandant les jalons (milestones) de suivi.
        :param return_photo: booléen demandant la photo de livraison (si disponible).
        :return: dict JSON de la réponse.
        """
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
            raise RuntimeError(f"Track failed {resp.status_code}: {resp.text}")
        payload = resp.json()
        # Enregistrer la réponse brute en base
        con = _db()
        con.execute(
            "INSERT INTO track_raw(tracking_number, fetched_at_utc, source, payload_json) VALUES(?,?,?,?)",
            (tracking_number, int(time.time()), "track_api", json.dumps(payload)),
        )
        con.commit()
        # Extraire les événements individuels et les stocker
        self._extract_and_store_events(tracking_number, payload)
        return payload

    def _extract_and_store_events(self, tracking_number: str, payload: Dict[str, Any]) -> None:
        """Parcourt la réponse JSON pour extraire les événements et les stocker en base."""
        con = _db()
        # Certaines réponses ont trackResponse.shipment[*].package[*].activity[*]
        shipments: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            shipments = (
                payload.get("trackResponse", {}).get("shipment", [])
                or payload.get("shipment", [])
            )
        inserted = 0
        for sh in shipments:
            packages = sh.get("package", []) if isinstance(sh, dict) else []
            for pkg in packages:
                activities = pkg.get("activity", []) if isinstance(pkg, dict) else []
                for act in activities:
                    # Dates fournies sous forme YYYYMMDD, HHMMSS
                    date_str = act.get("date") or act.get("gmtDate") or ""
                    time_str = act.get("time") or act.get("gmtTime") or ""
                    event_ts = ""
                    if date_str and len(date_str) == 8 and time_str and len(time_str) >= 6:
                        event_ts = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                    # Localisation
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
                                tracking_number,
                                event_ts,
                                event_city,
                                event_state,
                                event_postal,
                                event_country,
                                status_type,
                                status_code,
                                status_desc,
                                raw_json
                            ) VALUES (?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                tracking_number,
                                event_ts,
                                city,
                                state,
                                postal,
                                country,
                                stype,
                                scode,
                                sdesc,
                                json.dumps(act),
                            ),
                        )
                        inserted += con.total_changes
                    except sqlite3.Error:
                        # Ignorer les erreurs d'insertion en doublon ou de champ manquant
                        pass
        con.commit()

    def timeline(self, tracking_number: str, limit: int = 50) -> List[Tuple]:
        """Retourne une liste d'événements classés du plus récent au plus ancien."""
        con = _db()
        cur = con.execute(
            """
            SELECT event_ts, status_type, status_code, status_desc,
                   event_city, event_state, event_postal, event_country
            FROM track_events
            WHERE tracking_number = ?
            ORDER BY event_ts DESC
            LIMIT ?
            """,
            (tracking_number, limit),
        )
        return cur.fetchall()

    def subscribe_track_alert(
        self,
        tracking_numbers: List[str],
        webhook_url: str,
        webhook_credential: str,
        locale: str = "en_US",
        country_code: str = "US",
    ) -> Dict[str, Any]:
        """
        Crée une ou plusieurs souscriptions aux alertes de suivi UPS.

        L'endpoint Track Alert accepte jusqu'à 100 numéros par requête. Le
        webhook doit répondre rapidement (HTTP 2xx) et peut être sécurisé via
        un token fourni dans l'en-tête `credential`.
        """
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
        # Enregistrer en base les numéros validés
        con = _db()
        for tn in payload.get("validTrackingNumbers", []) or []:
            con.execute(
                "INSERT OR IGNORE INTO subscriptions(tracking_number, created_at_utc, env) VALUES(?,?,?)",
                (tn, int(time.time()), self.cfg.env),
            )
        con.commit()
        return payload


def main() -> None:
    import argparse
    # Chargement des paramètres d'authentification
    client_id = os.environ.get("UPS_CLIENT_ID", "").strip()
    client_secret = os.environ.get("UPS_CLIENT_SECRET", "").strip()
    env = os.environ.get("UPS_ENV", "prod").strip().lower() or "prod"
    if env not in ("prod", "cie"):
        env = "prod"
    if not client_id or not client_secret:
        raise SystemExit(
            "❌ Configurez UPS_CLIENT_ID et UPS_CLIENT_SECRET dans un fichier .env ou vos variables d'environnement."
        )
    cfg = UpsConfig(client_id=client_id, client_secret=client_secret, env=env)
    tracker = UpsTracker(cfg)

    parser = argparse.ArgumentParser(description="UPS tracker hyper détaillé (API officielle)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # Sous-commande fetch
    p_fetch = sub.add_parser(
        "fetch", help="Récupère les informations de suivi pour un numéro UPS"
    )
    p_fetch.add_argument("tracking_number", help="Numéro de suivi (1Z...)")
    p_fetch.add_argument(
        "--pod", action="store_true", help="Inclure la preuve de livraison (POD), si autorisé"
    )
    p_fetch.add_argument(
        "--sig", action="store_true", help="Inclure l'image de signature, si autorisé"
    )
    p_fetch.add_argument(
        "--milestones", action="store_true", default=True, help="Inclure les jalons (milestones)"
    )
    p_fetch.add_argument(
        "--photo", action="store_true", help="Inclure la photo de livraison, si disponible"
    )

    # Sous-commande timeline
    p_tl = sub.add_parser(
        "timeline", help="Affiche les événements stockés d'un numéro de suivi"
    )
    p_tl.add_argument("tracking_number")
    p_tl.add_argument("--limit", type=int, default=50, help="Nombre d'événements à afficher")

    # Sous-commande subscribe
    p_sub = sub.add_parser(
        "subscribe", help="Crée un abonnement Track Alert pour un ou plusieurs numéros"
    )
    p_sub.add_argument("tracking_numbers", nargs="+", help="Un ou plusieurs numéros de suivi")
    p_sub.add_argument(
        "--webhook-url",
        default=os.environ.get("TRACK_ALERT_WEBHOOK_URL", ""),
        help="URL du webhook qui recevra les notifications",
    )
    p_sub.add_argument(
        "--webhook-credential",
        default=os.environ.get("TRACK_ALERT_WEBHOOK_CREDENTIAL", ""),
        help="Jeton partagé transmis dans l'en-tête 'credential'",
    )
    args = parser.parse_args()

    if args.cmd == "fetch":
        payload = tracker.fetch_tracking_details(
            args.tracking_number,
            return_signature=args.sig,
            return_pod=args.pod,
            return_milestones=args.milestones,
            return_photo=args.photo,
        )
        # Afficher la réponse formatée pour inspection
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        print(
            f"\n✅ OK. Les événements ont été stockés. Utilisez la commande 'timeline {args.tracking_number}' pour voir la timeline."
        )
    elif args.cmd == "timeline":
        rows = tracker.timeline(args.tracking_number, limit=args.limit)
        if not rows:
            print(
                "😶 Aucun événement en base pour ce numéro. Lancez d'abord 'fetch' pour récupérer les informations."
            )
            return
        for (ts, stype, scode, sdesc, city, state, postal, country) in rows:
            where = " / ".join([x for x in [city, state, postal, country] if x])
            print(f"- {ts or '??'} | {stype or '?'}-{scode or '?'} | {sdesc or ''} | {where}")
    elif args.cmd == "subscribe":
        if not args.webhook_url or not args.webhook_credential:
            raise SystemExit(
                "❌ Spécifiez --webhook-url et --webhook-credential ou configurez TRACK_ALERT_WEBHOOK_URL et TRACK_ALERT_WEBHOOK_CREDENTIAL dans .env"
            )
        payload = tracker.subscribe_track_alert(
            args.tracking_numbers,
            args.webhook_url,
            args.webhook_credential,
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        print(
            "\n✅ Abonnement créé. Vérifiez votre serveur webhook pour les notifications entrantes."
        )


if __name__ == "__main__":
    main()
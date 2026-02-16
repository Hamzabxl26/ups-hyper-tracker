"""
Serveur de réception pour les notifications UPS Track Alert
----------------------------------------------------------

Ce serveur FastAPI minimal reçoit les notifications Track Alert envoyées par UPS
et les stocke dans une base SQLite locale. Vous pouvez adapter cet exemple pour
déclencher des actions supplémentaires (envoi d'e-mails, mises à jour CRM,
notifications Slack, etc.) dès qu'un événement est reçu.

Configuration :

* Définissez `UPS_DB` dans votre `.env` pour utiliser la même base que
  `ups_tracker_codex.py` ou une autre si vous préférez. Le serveur crée
  automatiquement la table `track_alert_events` si elle n'existe pas.
* Définissez `TRACK_ALERT_WEBHOOK_CREDENTIAL` pour vérifier le jeton transmis
  par UPS dans l'en-tête `credential` de chaque requête.

Exécution :

```bash
uvicorn webhook_server:app --host 0.0.0.0 --port 8000
```

Vous pouvez tester la réception en envoyant une requête POST JSON à
`http://localhost:8000/ups/webhook` avec un en-tête `credential` égal au secret
défini et un corps arbitraire. Le serveur répondra `{"ok": true}` pour
indique que le message a été reçu et enregistré.
"""

import os
import time
import json
import sqlite3
from fastapi import FastAPI, Header, HTTPException, Request


app = FastAPI()


def _db() -> sqlite3.Connection:
    """Obtient un connecteur vers la base définie par UPS_DB et crée la table d'événements."""
    db_path = os.environ.get("UPS_DB", "ups_tracking.sqlite3")
    con = sqlite3.connect(db_path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS track_alert_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at_utc INTEGER NOT NULL,
            credential TEXT,
            payload_json TEXT NOT NULL
        )
        """
    )
    con.commit()
    return con


EXPECTED_CREDENTIAL = os.environ.get("TRACK_ALERT_WEBHOOK_CREDENTIAL", "")


@app.post("/ups/webhook")
async def ups_webhook(request: Request, credential: str = Header(default="")):
    """Point d'entrée pour recevoir les notifications Track Alert d'UPS."""
    if EXPECTED_CREDENTIAL and credential != EXPECTED_CREDENTIAL:
        raise HTTPException(status_code=401, detail="Bad credential")
    payload = await request.body()
    # Essayer de décoder en JSON, sinon stocker tel quel
    try:
        payload_json = json.loads(payload.decode())
    except Exception:
        payload_json = {"raw": payload.decode(errors="ignore")}
    con = _db()
    con.execute(
        "INSERT INTO track_alert_events(received_at_utc, credential, payload_json) VALUES(?,?,?)",
        (int(time.time()), credential, json.dumps(payload_json)),
    )
    con.commit()
    # Réponse rapide pour indiquer la réception
    return {"ok": True}
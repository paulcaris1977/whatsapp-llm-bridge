# whatsapp_mcp.py — v1.3.0
# Changelog :
#   v1.0.0 — get_whatsapp_messages, get_whatsapp_contacts, get_whatsapp_history
#   v1.1.0 — ajout send_whatsapp_message
#   v1.2.0 — ajout get_pro_messages (classification LLM pro/perso via Anthropic API)
#   v1.3.0 — ajout list_whatsapp_chats (GET /chats + filtre dynamique LLM pro/perso)

import os
import json
import httpx
from anthropic import Anthropic
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("WhatsApp Bridge")

# Compatibilité avec le .env existant (API_KEY) + fallback hardcodé pour Claude Desktop
API_KEY = os.getenv("WHATSAPP_API_KEY", os.getenv("API_KEY", "a3f8c2e1d4b7a9f0e5c8d2b1a4f7e0c3d6b9a2f5e8c1d4b7a0f3e6c9d2b5a8f1"))
BASE_URL = os.getenv("WHATSAPP_BASE_URL", "http://localhost:8000")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None

CLASSIFICATION_PROMPT = """
Tu es un expert en classification de conversations WhatsApp pour Taranis Cooperage Group.

RÈGLES DE CLASSIFICATION :
Un chat est "PRO" si :
- Il concerne l'activité professionnelle de Taranis Cooperage (vente, achat, production ou logistique de tonneaux en chêne, fûts, barriques, etc.)
- Les interlocuteurs sont : clients, prospects, fournisseurs, transporteurs, partenaires, comptables, banques, collègues, employés
- Les sujets typiques : devis, commandes, prix, délais de livraison, spécifications techniques (225L, 228L, neuves, d'occasion, etc.), facturation, paiement, suivi de production, réclamations clients, RDV professionnels
- Il y a des références à des montants, des quantités, des numéros de commande, des bons de livraison, des RDV d'affaires
- Le ton est professionnel ou neutre-commercial

Un chat est "PERSO" si :
- Il concerne la vie privée : famille, amis, loisirs, sport, vacances, santé, blagues
- Les sujets sont : week-end, soirées, enfants, voitures, maison, cuisine, films, musique, politique
- Le ton est décontracté, avec des émojis excessifs, des "haha", "mdr"

Règles de décision :
- Un seul message personnel dans un chat majoritairement professionnel ne rend pas le chat "perso"
- Un chat est classé selon sa nature dominante et son contexte global
- En cas de doute, classe en "pro"

Réponds UNIQUEMENT par un JSON valide :
{"category": "pro" ou "perso", "confidence": 0.95, "reason": "Explication courte en une phrase"}
"""


@mcp.tool()
def get_whatsapp_messages(limit: int = 50, after_date: str = "", before_date: str = "", contact: str = "") -> str:
    """
    Récupère les messages WhatsApp.
    - limit: nombre maximum de messages (défaut 50, max 500)
    - after_date: filtre à partir d'une date (format: YYYY-MM-DD, ex: 2026-06-01)
    - before_date: filtre jusqu'à une date (format: YYYY-MM-DD, ex: 2026-06-12)
    - contact: filtre par numéro de téléphone (ex: 33612345678)
    """
    params = {"limit": limit}
    if after_date:
        params["after_date"] = after_date
    if before_date:
        params["before_date"] = before_date
    if contact:
        params["contact"] = contact
    with httpx.Client(timeout=30.0) as client:
        response = client.get(f"{BASE_URL}/messages", headers={"X-API-Key": API_KEY}, params=params)
        response.raise_for_status()
        return json.dumps(response.json(), ensure_ascii=False, indent=2)


@mcp.tool()
def get_whatsapp_history(after_date: str, before_date: str = "", contact: str = "", page_size: int = 500) -> str:
    """
    Récupère l'historique complet WhatsApp par pagination automatique.
    - after_date: date de début (format: YYYY-MM-DD)
    - before_date: date de fin optionnelle (format: YYYY-MM-DD), défaut = aujourd'hui
    - contact: filtre par numéro optionnel
    - page_size: taille de chaque page (max 500)
    """
    all_messages = []
    page = 1
    cursor = before_date if before_date else ""

    while True:
        params = {"limit": page_size, "after_date": after_date}
        if cursor:
            params["before_date"] = cursor
        if contact:
            params["contact"] = contact

        with httpx.Client(timeout=30.0) as client:
            response = client.get(f"{BASE_URL}/messages", headers={"X-API-Key": API_KEY}, params=params)
            msgs = response.json()

        if not msgs:
            break

        all_messages.extend(msgs)
        oldest = msgs[-1]["timestamp"]
        next_cursor = oldest[:10]

        if next_cursor >= (cursor if cursor else "9999-99-99"):
            break
        if next_cursor <= after_date:
            break

        cursor = next_cursor
        page += 1

        if page > 20:
            break

    return json.dumps({"total": len(all_messages), "pages": page, "messages": all_messages}, ensure_ascii=False, indent=2)


@mcp.tool()
def get_whatsapp_contacts() -> str:
    """Récupère le mapping numéro → nom des contacts WhatsApp."""
    with httpx.Client(timeout=30.0) as client:
        response = client.get(f"{BASE_URL}/contacts", headers={"X-API-Key": API_KEY})
        response.raise_for_status()
        return json.dumps(response.json(), ensure_ascii=False, indent=2)


@mcp.tool()
def send_whatsapp_message(to: str, message: str) -> str:
    """
    Envoie un message WhatsApp à un contact.
    - to: numéro de téléphone au format international E.164 (ex: +33672573764)
    - message: texte du message à envoyer
    IMPORTANT : toujours demander confirmation à l'utilisateur avant d'envoyer.
    """
    with httpx.Client(timeout=15.0) as client:
        response = client.post(f"{BASE_URL}/send", headers={"X-API-Key": API_KEY}, json={"to": to, "message": message})
        response.raise_for_status()
        return json.dumps(response.json(), ensure_ascii=False, indent=2)


@mcp.tool()
def get_pro_messages(limit: int = 200, after_date: str = "") -> str:
    """
    Récupère et classe les messages WhatsApp professionnels via LLM.
    Filtre automatiquement les conversations pro des conversations perso.
    - limit: nombre de messages à analyser (défaut 200)
    - after_date: filtre à partir d'une date (format: YYYY-MM-DD)
    Retourne uniquement les chats classés PRO avec confidence >= 0.7.
    """
    if not anthropic_client:
        return json.dumps({"error": "ANTHROPIC_API_KEY non configuré"}, indent=2)

    params = {"limit": limit}
    if after_date:
        params["after_date"] = after_date

    with httpx.Client(timeout=60.0) as client:
        resp = client.get(f"{BASE_URL}/messages", headers={"X-API-Key": API_KEY}, params=params)
        resp.raise_for_status()
        messages = resp.json()

    # Regrouper par chat_jid (max 50 chats)
    chats = {}
    for msg in messages:
        jid = msg.get("chat_jid", "unknown")
        chats.setdefault(jid, []).append(msg)

    chat_jids = list(chats.keys())[:50]
    pro_chats = []

    for jid in chat_jids:
        chat_msgs = sorted(chats[jid], key=lambda x: x.get("timestamp", ""), reverse=True)[:15]
        text_block = "\n".join(
            f"{m.get('sender', '')}: {m.get('content', '')}" for m in chat_msgs
        )

        try:
            completion = anthropic_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                temperature=0.0,
                system=CLASSIFICATION_PROMPT,
                messages=[{"role": "user", "content": f"Chat JID: {jid}\n\nMessages:\n{text_block}"}],
            )
            content = completion.content[0].text.strip()
            if "{" in content:
                content = content[content.find("{"): content.rfind("}") + 1]
            classification = json.loads(content)
        except Exception:
            classification = {"category": "pro", "confidence": 0.5, "reason": "Parse error - classé pro par défaut"}

        if classification.get("category") == "pro" and classification.get("confidence", 0) >= 0.7:
            pro_chats.append({
                "chat_jid": jid,
                "category": classification["category"],
                "confidence": classification["confidence"],
                "reason": classification.get("reason", ""),
                "message_count": len(chat_msgs),
                "messages": chat_msgs,
            })

    return json.dumps({
        "total_chats": len(chat_jids),
        "pro_chats": len(pro_chats),
        "chats": pro_chats,
    }, ensure_ascii=False, indent=2)



# JIDs exclus définitivement (groupes perso, newsletters, spam)
EXCLUDED_JIDS = {
    "33672571698-1633559683@g.us",   # groupe perso français
    "120363160863168828@g.us",        # exclu permanent
    "120363180365005349@newsletter",  # newsletter
    "status@broadcast",               # statuts WhatsApp
}

# Nom de chats clairement perso → exclusion directe sans LLM
PERSO_KEYWORDS = ["famille", "family", "perso", "amis", "friends", "vacances"]


def _classify_chat_by_name_and_content(chat_jid: str, chat_name: str, recent_messages: list) -> dict:
    """Classification LLM d'un chat basée sur son nom + ses messages récents."""
    # Exclusion directe par JID
    if chat_jid in EXCLUDED_JIDS:
        return {"category": "perso", "confidence": 1.0, "reason": "JID exclu définitivement"}

    # Exclusion directe par nom (mots-clés perso évidents)
    if any(kw in chat_name.lower() for kw in PERSO_KEYWORDS):
        return {"category": "perso", "confidence": 0.95, "reason": f"Nom du chat contient un mot-clé perso : {chat_name}"}

    if not anthropic_client:
        return {"category": "pro", "confidence": 0.5, "reason": "Pas de client Anthropic - classé pro par défaut"}

    text_block = "\n".join(
        f"{m.get('sender', '')}: {m.get('content', '')}"
        for m in recent_messages[:15]
        if m.get('content', '').strip()
    ) or f"[Pas de messages texte — nom du chat : {chat_name}]"

    try:
        completion = anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            temperature=0.0,
            system=CLASSIFICATION_PROMPT,
            messages=[{"role": "user", "content": f"Chat JID: {chat_jid}\nNom du chat: {chat_name}\n\nMessages récents:\n{text_block}"}],
        )
        content = completion.content[0].text.strip()
        if "{" in content:
            content = content[content.find("{"): content.rfind("}") + 1]
        return json.loads(content)
    except Exception:
        return {"category": "pro", "confidence": 0.5, "reason": "Parse error - classé pro par défaut"}


@mcp.tool()
def list_whatsapp_chats(pro_only: bool = True) -> str:
    """
    Liste tous les chats WhatsApp avec leur nom réel.
    Utilise un filtre LLM dynamique pour séparer pro/perso.
    - pro_only: si True (défaut), retourne uniquement les chats PRO (confidence >= 0.7)
                si False, retourne tous les chats avec leur classification
    Les JIDs exclus définitivement (perso, newsletters) sont toujours filtrés.
    """
    # 1. Récupérer tous les chats avec noms
    with httpx.Client(timeout=60.0) as client:
        resp = client.get(f"{BASE_URL}/chats", headers={"X-API-Key": API_KEY})
        resp.raise_for_status()
        chats = resp.json()

    # 2. Récupérer les messages récents pour la classification
    with httpx.Client(timeout=60.0) as client:
        resp = client.get(f"{BASE_URL}/messages", headers={"X-API-Key": API_KEY}, params={"limit": 500})
        resp.raise_for_status()
        all_messages = resp.json()

    # Indexer les messages par chat_jid
    messages_by_jid = {}
    for msg in all_messages:
        jid = msg.get("chat_jid", "")
        messages_by_jid.setdefault(jid, []).append(msg)

    # 3. Classifier chaque chat
    results = []
    for chat in chats:
        jid = chat["chat_jid"]
        name = chat["name"]

        # Exclure les JIDs sans nom résolu (JID brut = groupe inconnu/inactif)
        if name == jid and "@g.us" in jid:
            continue

        recent_msgs = messages_by_jid.get(jid, [])
        classification = _classify_chat_by_name_and_content(jid, name, recent_msgs)

        entry = {
            "chat_jid": jid,
            "name": name,
            "type": chat["type"],
            "category": classification["category"],
            "confidence": classification["confidence"],
            "reason": classification.get("reason", ""),
        }

        if pro_only:
            if classification["category"] == "pro" and classification["confidence"] >= 0.7:
                results.append(entry)
        else:
            results.append(entry)

    return json.dumps({
        "total": len(chats),
        "returned": len(results),
        "pro_only": pro_only,
        "chats": results,
    }, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    mcp.run(transport="stdio")

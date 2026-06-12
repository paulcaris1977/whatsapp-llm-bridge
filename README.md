# whatsapp-llm-bridge

Donne à un LLM (Claude, GPT, etc.) un accès en lecture — et bientôt en écriture — à ton compte WhatsApp personnel.

## Ce que ça fait

- **Lire** les messages reçus sur ton compte WhatsApp via une API REST sécurisée
- **Stocker** les messages localement dans SQLite (aucune donnée envoyée à un tiers)
- **Exposer** une API que ton LLM peut appeler pour lire les conversations

## Ce qui arrive ensuite (phase 2)

- Permettre au LLM de **rédiger et envoyer des réponses** sur ton compte, à ta demande explicite

## Ce que ça ne fait PAS — et ne fera jamais

| Action interdite | Pourquoi |
|---|---|
| Envoyer des messages à des inconnus | Ban WhatsApp quasi-certain |
| Envoyer en masse (bulk/broadcast) | Violation des CGU WhatsApp |
| Scraper des groupes publics | Violation des CGU WhatsApp |
| Automatiser des envois sans validation humaine | Risque de ban + usage abusif |

> WhatsApp détecte les comportements automatisés anormaux. Ce projet est conçu pour un usage **personnel et conversationnel uniquement** : lire tes messages et répondre à des contacts existants, un par un, après validation.

## Architecture

```
ton compte WhatsApp
        │
        │  (whatsmeow — bibliothèque Go open source)
        ▼
  Go bridge :8081          ← reçoit et stocke les messages (SQLite)
        │
        │  (HTTP interne, non exposé)
        ▼
  FastAPI :8000             ← API sécurisée par clé (X-API-Key)
        │
        │  (appel HTTP)
        ▼
  ton LLM (Claude, etc.)    ← lit les messages, prépare des réponses
```

## Démarrage rapide

```bash
# 1. Copier et remplir les variables d'environnement
cp .env.example .env

# 2. Générer deux clés API distinctes
python -c "import secrets; print(secrets.token_hex(32))"  # → API_KEY
python -c "import secrets; print(secrets.token_hex(32))"  # → INTERNAL_API_KEY

# 3. Lancer
docker-compose up --build
```

## Première connexion WhatsApp

Au premier démarrage, un QR code s'affiche dans les logs :

```bash
docker-compose logs -f bridge
```

Scanne-le avec WhatsApp (Appareils connectés → Connecter un appareil). La session est ensuite persistée dans `./store/`.

## Endpoints disponibles

| Méthode | Endpoint | Description |
|---|---|---|
| GET | `/messages` | 50 derniers messages reçus |
| GET | `/health` | État du service |

Toutes les requêtes nécessitent le header `X-API-Key: <API_KEY>`.

## Variables d'environnement

| Variable | Description |
|---|---|
| `API_KEY` | Clé pour les appels externes (ton LLM) |
| `INTERNAL_API_KEY` | Clé interne FastAPI → Go bridge (ne pas partager) |
| `BRIDGE_URL` | URL du Go bridge (défaut : `http://localhost:8081`) |

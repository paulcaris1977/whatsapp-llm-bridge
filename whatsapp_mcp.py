import httpx
import json
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("WhatsApp Bridge")

API_KEY = "a3f8c2e1d4b7a9f0e5c8d2b1a4f7e0c3d6b9a2f5e8c1d4b7a0f3e6c9d2b5a8f1"
BASE_URL = "http://localhost:8000"

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

    with httpx.Client() as client:
        response = client.get(
            f"{BASE_URL}/messages",
            headers={"X-API-Key": API_KEY},
            params=params
        )
        return json.dumps(response.json(), ensure_ascii=False, indent=2)

@mcp.tool()
def get_whatsapp_history(after_date: str, before_date: str = "", contact: str = "", page_size: int = 500) -> str:
    """
    Récupère l'historique complet WhatsApp par pagination automatique.
    Appelle get_whatsapp_messages en boucle en utilisant before_date comme curseur.
    - after_date: date de début (format: YYYY-MM-DD)
    - before_date: date de fin optionnelle (format: YYYY-MM-DD), défaut = aujourd'hui
    - contact: filtre par numéro optionnel
    - page_size: taille de chaque page (max 500)
    Retourne tous les messages avec métadonnées de pagination.
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
            response = client.get(
                f"{BASE_URL}/messages",
                headers={"X-API-Key": API_KEY},
                params=params
            )
            msgs = response.json()

        if not msgs:
            break

        all_messages.extend(msgs)

        # Le curseur devient le timestamp du dernier message (le plus ancien)
        oldest = msgs[-1]["timestamp"]
        # Extraire la date du timestamp ISO pour la prochaine page
        next_cursor = oldest[:10]  # YYYY-MM-DD

        # Si le curseur n'a pas changé ou on a tout récupéré, on arrête
        if next_cursor >= (cursor if cursor else "9999-99-99"):
            break
        if next_cursor <= after_date:
            break

        cursor = next_cursor
        page += 1

        # Sécurité : max 20 pages
        if page > 20:
            break

    return json.dumps({
        "total": len(all_messages),
        "pages": page,
        "messages": all_messages
    }, ensure_ascii=False, indent=2)

@mcp.tool()
def get_whatsapp_contacts() -> str:
    """Récupère le mapping numéro → nom des contacts WhatsApp."""
    with httpx.Client() as client:
        response = client.get(
            f"{BASE_URL}/contacts",
            headers={"X-API-Key": API_KEY},
        )
        return json.dumps(response.json(), ensure_ascii=False, indent=2)

if __name__ == "__main__":
    mcp.run(transport="stdio")

# main.py — FastAPI + aiogram v3.x (compatible Render webhook)
import os
import re
from typing import List, Dict, Optional
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import CommandStart
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Update,
    CallbackQuery, Message
)

# ----------------- Configuration -----------------
TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    raise RuntimeError("Missing TELEGRAM_TOKEN environment variable")

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()
app = FastAPI()

# -------------------------------------------------
# Stockage en mémoire (à remplacer par PostgreSQL plus tard)
# -------------------------------------------------
BASES: Dict[str, Dict] = {
    "default": {"records": 0, "size_mb": 0.0, "last_import": None, "phone_count": 0}
}
USER_PREFS: Dict[int, Dict] = {}
USER_STATE: Dict[int, Dict] = {}

def ensure_user(user_id: int) -> None:
    USER_PREFS.setdefault(user_id, {"active_db": "default"})
    USER_STATE.setdefault(user_id, {})

def get_active_db(user_id: int) -> str:
    return USER_PREFS.get(user_id, {}).get("active_db", "default")

def set_active_db(user_id: int, dbname: str) -> None:
    USER_PREFS.setdefault(user_id, {})["active_db"] = dbname

# ----------------- Utilitaires parsing TXT -----------------
def normalize_phone(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    s = re.sub(r"^(?:\+33|0033)\s*", "0", s)
    digits = re.sub(r"\D", "", s)
    if len(digits) == 9:
        digits = "0" + digits
    return digits or None

def parse_txt_block(block: str) -> Optional[Dict]:
    lines = [l.strip() for l in block.splitlines() if l.strip()]
    if not lines:
        return None

    data = {
        "iban": None, "bic": None, "full_name_raw": None,
        "first_name": None, "last_name": None, "dob": None,
        "email": None, "statut": None, "adresse": None,
        "ville": None, "cp": None, "mobile": None, "voip": None,
    }

    re_kv = re.compile(r"^\s*([A-Za-zÉÈÊËÀÂÄÔÖÎÏÛÜÇéèêëàâäôöîïûüç\s/.-]+)\s*:\s*(.+?)\s*$")
    re_iban = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{11,}$")
    re_cp = re.compile(r".*?\((\d{5})\)\s*$")

    i = 0
    while i < len(lines):
        line = lines[i]
        if line.upper().startswith("IBAN"):
            m = re_kv.match(line)
            if m:
                v = m.group(2).strip().replace(" ", "")
                if re_iban.match(v):
                    data["iban"] = v
            i += 1
            continue
        if line.upper().startswith("BIC"):
            m = re_kv.match(line)
            if m:
                data["bic"] = m.group(2).strip()
            i += 1
            continue
        if ":" not in line:
            data["full_name_raw"] = line
            parts = re.split(r"\s*[-/]\s*", line, maxsplit=1)
            if len(parts) == 2:
                data["last_name"], data["first_name"] = parts[0].strip(), parts[1].strip()
            else:
                data["last_name"] = line.strip()
            i += 1
            break
        i += 1

    while i < len(lines):
        line = lines[i]
        m = re_kv.match(line)
        if m:
            key = m.group(1).strip().lower()
            val = m.group(2).strip()
            if val.upper() == "N/A":
                val = None
            if key.startswith("dob") or "naiss" in key:
                data["dob"] = val
            elif key.startswith("email"):
                data["email"] = val
            elif key.startswith("statut"):
                data["statut"] = val
            elif key.startswith("adresse"):
                data["adresse"] = val
            elif key.startswith("ville"):
                mcp = re_cp.match(val or "")
                if mcp:
                    data["cp"] = mcp.group(1)
                    data["ville"] = (val or "")[: (val or "").rfind("(")].strip()
                else:
                    data["ville"] = val
            elif key.startswith("mobile"):
                data["mobile"] = normalize_phone(val)
            elif key.startswith("voip"):
                data["voip"] = normalize_phone(val)
            elif key.startswith("iban") and not data["iban"]:
                v = (val or "").replace(" ", "")
                if re_iban.match(v):
                    data["iban"] = v
            elif key.startswith("bic") and not data["bic"]:
                data["bic"] = val
        i += 1

    if not any([data["mobile"], data["voip"], data["email"], data["full_name_raw"], data["iban"]]):
        return None
    return data

def parse_txt_blocks(content: str) -> List[Dict]:
    blocks = re.split(r"(?:\r?\n){2,}", content)
    results = []
    for b in blocks:
        rec = parse_txt_block(b)
        if rec:
            results.append(rec)
    return results

# ----------------- Accueil (/start) -----------------
@router.message(CommandStart())
async def accueil(message: types.Message):
    user_id = message.from_user.id
    ensure_user(user_id)
    active_db = get_active_db(user_id)

    nb_contactes = 0
    nb_appels_manques = 0
    nb_dossiers_en_cours = 0
    nb_numeros = BASES.get(active_db, {}).get("phone_count", 0)

    text = (
        "👋 Bienvenue sur *FICHES CLIENTS*\n\n"
        f"Base active : `{active_db}`\n\n"
        "*Statistiques du jour :*\n"
        f"- Clients traités : {nb_contactes}\n"
        f"- Appels manqués : {nb_appels_manques}\n"
        f"- Dossiers en cours : {nb_dossiers_en_cours}\n"
        f"- Numéros enregistrés : {nb_numeros}\n\n"
        "_Utilisez les boutons ci-dessous ou tapez /start pour revenir à l'accueil._"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Gérer les bases", callback_data="home:db")],
        [InlineKeyboardButton(text="Rechercher une fiche", callback_data="home:search")],
        [InlineKeyboardButton(text=f"Appels manqués ({nb_appels_manques})", callback_data="home:missed")],
        [InlineKeyboardButton(text=f"Dossiers en cours ({nb_dossiers_en_cours})", callback_data="home:cases")],
    ])

    image_url = "https://i.postimg.cc/0jNN08J5/IMG-0294.jpg"
    await message.answer_photo(photo=image_url, caption=text, parse_mode="Markdown", reply_markup=kb)

dp.include_router(router)

# ----------------- FastAPI: webhook -----------------
app = FastAPI()

@app.get("/")
async def health():
    return {"status": "ok"}

@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    try:
        update = Update.model_validate(data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Bad Update: {e}")
    await dp.feed_update(bot, update)
    return {"ok": True}

# ==========================================================
# GÉRER LES BASES + IMPORT (même version que précédente)
# ==========================================================
def render_db_list_text(user_id: int) -> str:
    active = get_active_db(user_id)
    lines = ["Bases disponibles :", ""]
    for name, meta in BASES.items():
        marker = " (active)" if name == active else ""
        lines.append(
            f"- {name}{marker} — {meta['records']} fiches — {meta['size_mb']} Mo — {meta.get('phone_count',0)} numéros"
        )
    lines.append("")
    lines.append("Choisissez une action.")
    return "\n".join(lines)

def db_list_keyboard(user_id: int) -> InlineKeyboardMarkup:
    active = get_active_db(user_id)
    rows = []
    for name in BASES.keys():
        rows.append([InlineKeyboardButton(text=f"{'●' if name == active else '○'} {name}",
                                          callback_data=f"db:use:{name}")])
        rows.append([
            InlineKeyboardButton(text="Stats", callback_data=f"db:stats:{name}"),
            InlineKeyboardButton(text="Importer", callback_data=f"db:import:{name}"),
            InlineKeyboardButton(text="Exporter", callback_data=f"db:export:{name}"),
            InlineKeyboardButton(text="Supprimer", callback_data=f"db:drop:{name}")
        ])
    rows.append([InlineKeyboardButton(text="Ajouter une base", callback_data="db:create")])
    rows.append([InlineKeyboardButton(text="Retour à l'accueil (/start)", callback_data="nav:start")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def edit_home_like(cb: CallbackQuery, text: str, kb: InlineKeyboardMarkup):
    try:
        await cb.message.edit_caption(caption=text, reply_markup=kb)
    except Exception:
        await cb.message.answer(text, reply_markup=kb)

# --- Gérer les bases ---
@router.callback_query(F.data == "home:db")
async def open_db_list(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    text = render_db_list_text(user_id)
    kb = db_list_keyboard(user_id)
    await edit_home_like(cb, text, kb)
    await cb.answer()

@router.callback_query(F.data == "nav:start")
async def back_to_start(cb: CallbackQuery):
    await cb.message.answer("Retour à l'accueil : tapez /start")
    await cb.answer()

# --- (autres handlers : use, stats, create, drop, import, etc.) ---
# (Ils restent identiques à la version précédente)

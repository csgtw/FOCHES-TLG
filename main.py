# main.py — FastAPI + aiogram v3.x (Render webhook)
import os
import re
import csv
from io import StringIO
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Update,
    CallbackQuery, Message, FSInputFile
)

# ----------------- Config -----------------
TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    raise RuntimeError("Missing TELEGRAM_TOKEN environment variable")

TZ = ZoneInfo("Europe/Madrid")
bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()

# ----------------- FastAPI -----------------
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

# ----------------- Mémoire (remplacer par DB plus tard) -----------------
BASES: Dict[str, Dict] = {
    "default": {
        "records": 0, "size_mb": 0.0, "last_import": None, "phone_count": 0,
        "records_list": [], "dept_counts": {}
    }
}
USER_PREFS: Dict[int, Dict] = {}
USER_STATE: Dict[int, Dict] = {}
# flags possibles:
# - USER_STATE[user_id]["awaiting_base_name"] = True/False
# - USER_STATE[user_id]["awaiting_import_for_base"] = <basename> or None
# - USER_STATE[user_id]["awaiting_search_number"] = True/False
# - USER_STATE[user_id]["awaiting_note_for"] = {"base": str, "rid": str, "chat_id": int, "message_id": int}

# Stats du jour par utilisateur
# USER_DAILY_STATS[user_id][datestr] = {"treated": int, "missed": int, "cases": int}
USER_DAILY_STATS: Dict[int, Dict[str, Dict[str, int]]] = {}

# Listes des fiches marquées par utilisateur et par base
# USER_TREATED[user_id][base]    = [rid, ...]
# USER_MISSED[user_id][base]     = [rid, ...]
# USER_INPROGRESS[user_id][base] = [rid, ...]
USER_TREATED: Dict[int, Dict[str, List[str]]] = {}
USER_MISSED: Dict[int, Dict[str, List[str]]] = {}
USER_INPROGRESS: Dict[int, Dict[str, List[str]]] = {}

def ensure_user(user_id: int) -> None:
    USER_PREFS.setdefault(user_id, {"active_db": "default"})
    USER_STATE.setdefault(user_id, {})
    USER_DAILY_STATS.setdefault(user_id, {})
    USER_TREATED.setdefault(user_id, {})
    USER_MISSED.setdefault(user_id, {})
    USER_INPROGRESS.setdefault(user_id, {})

def get_active_db(user_id: int) -> str:
    return USER_PREFS.get(user_id, {}).get("active_db", "default")

def set_active_db(user_id: int, dbname: str) -> None:
    USER_PREFS.setdefault(user_id, {})["active_db"] = dbname
    USER_TREATED[user_id].setdefault(dbname, [])
    USER_MISSED[user_id].setdefault(dbname, [])
    USER_INPROGRESS[user_id].setdefault(dbname, [])

def today_str() -> str:
    return datetime.now(TZ).date().isoformat()

def get_today_stats(user_id: int) -> Dict[str, int]:
    ensure_user(user_id)
    d = today_str()
    bucket = USER_DAILY_STATS[user_id].setdefault(d, {"treated": 0, "missed": 0, "cases": 0})
    for k in ("treated", "missed", "cases"):
        bucket.setdefault(k, 0)
    return bucket

def inc_stat(user_id: int, key: str, delta: int = 1) -> None:
    if key not in ("treated", "missed", "cases"):
        return
    stats = get_today_stats(user_id)
    stats[key] = stats.get(key, 0) + delta

# ----------------- Utils -----------------
def normalize_phone(s: Optional[str]) -> Optional[str]:
    """0XXXXXXXXX (FR)."""
    if not s:
        return None
    s = s.strip()
    s = re.sub(r"^\s*\+33\s*", "0", s)
    s = re.sub(r"^\s*0033\s*", "0", s)
    digits = re.sub(r"\D", "", s)
    if digits.startswith("0") and len(digits) >= 10:
        digits = digits[:10]
        return digits if len(digits) == 10 else None
    if len(digits) == 9 and not digits.startswith("0"):
        digits = "0" + digits
    return digits if len(digits) == 10 and digits.startswith("0") else None

def dept_from_cp(cp: Optional[str]) -> Optional[str]:
    if not cp or not re.fullmatch(r"\d{5}", cp):
        return None
    if cp.startswith(("97", "98")):
        return cp[:3]
    return cp[:2]

def parse_txt_block(block: str) -> Optional[Dict]:
    lines = [l.strip() for l in block.splitlines() if l.strip()]
    if not lines:
        return None

    data = {
        "rid": None,
        "iban": None, "bic": None, "full_name_raw": None,
        "first_name": None, "last_name": None, "dob": None,
        "email": None, "statut": None, "adresse": None,
        "ville": None, "cp": None, "mobile": None, "voip": None,
        "notes": []
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
            i += 1; continue
        if line.upper().startswith("BIC"):
            m = re_kv.match(line)
            if m:
                data["bic"] = m.group(2).strip()
            i += 1; continue
        if ":" not in line:
            data["full_name_raw"] = line
            parts = re.split(r"\s*[-/]\s*", line, maxsplit=1)
            if len(parts) == 2:
                data["last_name"], data["first_name"] = parts[0].strip(), parts[1].strip()
            else:
                data["last_name"] = line.strip()
            i += 1; break
        i += 1

    while i < len(lines):
        line = lines[i]
        m = re_kv.match(line)
        if m:
            key = m.group(1).strip().lower()
            val = m.group(2).strip()
            if val and val.upper() == "N/A":
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

# ----------------- Helpers callback & IDs -----------------
async def safe_cb_answer(cb: CallbackQuery, text: Optional[str] = None):
    try:
        await cb.answer(text=text)
    except Exception:
        pass

def ensure_record_ids(base_name: str):
    """Assigne un rid unique à chaque fiche d'une base si manquant."""
    base = BASES.get(base_name, {})
    lst = base.get("records_list", [])
    for idx, r in enumerate(lst):
        if not r.get("rid"):
            r["rid"] = str(idx)

# Une seule page à la fois (pour la navigation)
async def show_page(cb: CallbackQuery, text: str, kb: InlineKeyboardMarkup,
                    photo_url: Optional[str] = None, parse_mode: Optional[str] = None):
    await safe_cb_answer(cb)
    try:
        await cb.message.delete()
    except Exception:
        pass
    if photo_url:
        await bot.send_photo(
            chat_id=cb.message.chat.id,
            photo=photo_url,
            caption=text,
            reply_markup=kb,
            parse_mode=parse_mode
        )
    else:
        await bot.send_message(
            chat_id=cb.message.chat.id,
            text=text,
            reply_markup=kb,
            parse_mode=parse_mode
        )

# ----------------- Rendu fiche + clavier -----------------
def pretty_name(rec: Dict) -> str:
    last = rec.get("last_name") or ""
    first = rec.get("first_name") or ""
    return (last + (" - " + first if first else "")) if (last or first) else (rec.get("full_name_raw") or "—")

def render_record_text(rec: Dict) -> str:
    name = pretty_name(rec)
    mobile = rec.get("mobile") or "—"
    voip = rec.get("voip") or "—"
    email = rec.get("email") or "—"
    ville = rec.get("ville") or "—"
    cp = rec.get("cp") or "—"
    adr = rec.get("adresse") or "—"
    iban = rec.get("iban") or "—"
    bic = rec.get("bic") or "—"
    notes_list = rec.get("notes") or []
    notes_block = ""
    if notes_list:
        lines = [f"- {n}" for n in notes_list[-10:]]  # montre les 10 dernières
        notes_block = "\n\nNotes :\n" + "\n".join(lines)
    return (
        "Fiche\n"
        f"- Nom : {name}\n"
        f"- Mobile : {mobile}\n"
        f"- VoIP : {voip}\n"
        f"- Email : {email}\n"
        f"- Adresse : {adr}\n"
        f"- Ville : {ville} ({cp})\n"
        f"- IBAN : {iban}\n"
        f"- BIC : {bic}"
        f"{notes_block}"
    )

def record_keyboard(base: str, rid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 En ligne",        callback_data=f"rec:ongoing:{base}:{rid}")],
        [InlineKeyboardButton(text="🟢 Fin d’appel",     callback_data=f"rec:finish:{base}:{rid}")],
        [InlineKeyboardButton(text="❌ Non traité",      callback_data=f"rec:missed:{base}:{rid}")],
        [InlineKeyboardButton(text="📝 Ajouter une note", callback_data=f"rec:note:{base}:{rid}")],
    ])

async def send_record_card(chat_id: int, base: str, rec: Dict):
    text = render_record_text(rec)
    kb = record_keyboard(base, rec.get("rid", "0"))
    await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)

def find_record(base: str, rid: str) -> Optional[Dict]:
    meta = BASES.get(base)
    if not meta:
        return None
    ensure_record_ids(base)
    for r in meta.get("records_list", []):
        if str(r.get("rid")) == str(rid):
            return r
    return None

# ----------------- Accueil -----------------
async def send_home(chat_id: int, user_id: int):
    active_db = get_active_db(user_id)
    stats = get_today_stats(user_id)
    nb_contactes = stats.get("treated", 0)
    nb_appels_manques = stats.get("missed", 0)
    nb_dossiers_en_cours_jour = stats.get("cases", 0)  # stats du jour
    nb_fiches = BASES.get(active_db, {}).get("records", 0)

    treated_count = len(USER_TREATED.get(user_id, {}).get(active_db, []))
    inprogress_count = len(USER_INPROGRESS.get(user_id, {}).get(active_db, []))

    text = (
        "👋 Bienvenue sur FICHES CLIENTS\n\n"
        f"Base active : {active_db}\n\n"
        "Statistiques du jour :\n"
        f"- Clients traités : {nb_contactes}\n"
        f"- Appels manqués : {nb_appels_manques}\n"
        f"- Dossiers en cours : {nb_dossiers_en_cours_jour}\n"
        f"- Fiches totales : {nb_fiches}\n\n"
        "Utilisez les boutons ci-dessous ou tapez /start pour revenir à l'accueil."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗄️ Gérer les bases", callback_data="home:db")],
        [InlineKeyboardButton(text="🔎 Rechercher une fiche", callback_data="home:search")],
        [InlineKeyboardButton(text=f"✅ Clients traités ({treated_count})", callback_data="home:treated")],
        [InlineKeyboardButton(text=f"🗂️ Dossiers en cours ({inprogress_count})", callback_data="home:cases")],
        [InlineKeyboardButton(text=f"📵 Appels manqués ({nb_appels_manques})", callback_data="home:missed")],
    ])

    image_url = "https://i.postimg.cc/0jNN08J5/IMG-0294.jpg"
    await bot.send_photo(chat_id=chat_id, photo=image_url, caption=text, reply_markup=kb)

@router.message(CommandStart())
async def accueil(message: types.Message):
    user_id = message.from_user.id
    ensure_user(user_id)
    await send_home(chat_id=message.chat.id, user_id=user_id)

dp.include_router(router)

# ----------------- Rechercher une fiche (bouton) -----------------
@router.callback_query(F.data == "home:search")
async def start_search(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    USER_STATE[user_id]["awaiting_search_number"] = True
    text = (
        "Recherche par numéro\n\n"
        "Envoie un numéro au format 06123456789.\n"
        "Je cherche dans la base active et j’affiche la fiche si elle existe."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Retour", callback_data="nav:start")]
    ])
    await show_page(cb, text, kb)

# ----------------- /num : recherche par numéro (commande) -----------------
async def find_and_reply_number(message: Message, raw_number: str):
    user_id = message.from_user.id
    ensure_user(user_id)
    active = get_active_db(user_id)

    num = normalize_phone(raw_number.strip())
    if not num or not re.fullmatch(r"0\d{9}", num):
        await message.answer("Numéro invalide. Exemple attendu : 06123456789")
        return

    base = BASES.get(active, {})
    records = base.get("records_list", [])
    ensure_record_ids(active)
    matches = [r for r in records if r.get("mobile") == num or r.get("voip") == num]

    if not matches:
        await message.answer(f"Aucune fiche trouvée pour le numéro {num}.")
        return

    if len(matches) == 1:
        await send_record_card(message.chat.id, active, matches[0])
        return

    # Plusieurs fiches : petit listing + boutons pour ouvrir
    lines = [f"{len(matches)} fiches trouvées pour {num} :", ""]
    buttons = []
    for r in matches[:10]:
        name = pretty_name(r)
        ville = r.get("ville") or "—"
        cp = r.get("cp") or "—"
        label = f"{name} — {ville} ({cp})"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"rec:view:{active}:{r['rid']}")])
    if len(matches) > 10:
        lines.append("…")

    await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.message(Command("num"))
async def search_by_number_cmd(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Utilisation : /num 06123456789")
        return
    await find_and_reply_number(message, parts[1])

# ----------------- Voir fiche via bouton (depuis listes) -----------------
@router.callback_query(F.data.startswith("rec:view:"))
async def rec_view(cb: CallbackQuery):
    # rec:view:<base>:<rid>
    try:
        _, _, base, rid = cb.data.split(":", 3)
    except Exception:
        return await safe_cb_answer(cb)
    rec = find_record(base, rid)
    if not rec:
        return await safe_cb_answer(cb, "Fiche introuvable.")
    await safe_cb_answer(cb)
    await send_record_card(cb.message.chat.id, base, rec)

# ----------------- Gérer les bases (UI simplifiée) -----------------
def render_db_list_text_only() -> str:
    return "Gérer les bases\n\nSélectionnez une base ci-dessous, ou ajoutez-en une nouvelle."

def db_list_keyboard(user_id: int) -> InlineKeyboardMarkup:
    active = get_active_db(user_id)
    rows = []
    for name in BASES.keys():
        label = f"{'●' if name == active else '○'} {name}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"db:open:{name}")])
    rows.append([InlineKeyboardButton(text="➕ Ajouter une base", callback_data="db:create")])
    rows.append([InlineKeyboardButton(text="Retour", callback_data="nav:start")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@router.callback_query(F.data == "home:db")
async def open_db_list(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    text = render_db_list_text_only()
    kb = db_list_keyboard(user_id)
    await show_page(cb, text, kb)

# ----------------- Menu d'une base -----------------
def base_menu_keyboard(name: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Importer (.txt/.csv/.jsonl)", callback_data=f"db:import:{name}")],
        [InlineKeyboardButton(text="📊 Statistiques (départements)", callback_data=f"db:stats:{name}")],
        [InlineKeyboardButton(text="📤 Exporter CSV", callback_data=f"db:export:{name}")],
        [InlineKeyboardButton(text="🗑️ Supprimer la base", callback_data=f"db:drop:{name}")],
        [InlineKeyboardButton(text="Retour", callback_data="home:db")],
    ])

@router.callback_query(F.data.startswith("db:open:"))
async def db_open(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await safe_cb_answer(cb, "Base introuvable.")
        return
    set_active_db(user_id, name)
    text = f"Base sélectionnée : {name}\n\nChoisissez une action."
    kb = base_menu_keyboard(name)
    await show_page(cb, text, kb)

# ----------------- Saisies texte : nom de base ou numéro recherché / note -----------------
@router.message(F.text)
async def capture_search_or_name_or_note(message: Message):
    user_id = message.from_user.id
    ensure_user(user_id)

    # Note en attente ?
    note_target = USER_STATE[user_id].get("awaiting_note_for")
    if note_target:
        base = note_target["base"]; rid = note_target["rid"]
        chat_id = note_target["chat_id"]; msg_id = note_target["message_id"]
        rec = find_record(base, rid)
        USER_STATE[user_id]["awaiting_note_for"] = None
        if not rec:
            await message.answer("Fiche introuvable pour ajouter la note.")
            return
        rec.setdefault("notes", []).append(message.text.strip())
        # Met à jour le message de la fiche avec la note ajoutée
        try:
            await bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
        except Exception:
            pass
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=render_record_text(rec),
                reply_markup=record_keyboard(base, rid)
            )
        except Exception:
            # si l’édition échoue (ex: message trop ancien), on renvoie une nouvelle fiche
            await send_record_card(message.chat.id, base, rec)
        await message.answer("✅ Note ajoutée.")
        return

    # Création base : on attend un nom
    if USER_STATE[user_id].get("awaiting_base_name"):
        raw = (message.text or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_]{1,40}", raw):
            await message.answer("Nom invalide. Autorisés: A–Z, a–z, 0–9, _. Max 40.")
            return
        if raw in BASES:
            await message.answer("Ce nom existe déjà. Choisissez-en un autre.")
            return

        BASES[raw] = {
            "records": 0, "size_mb": 0.0, "last_import": None, "phone_count": 0,
            "records_list": [], "dept_counts": {}
        }
        USER_STATE[user_id]["awaiting_base_name"] = False
        set_active_db(user_id, raw)

        USER_STATE[user_id]["awaiting_import_for_base"] = raw
        text = (
            f"Base créée : {raw}\n\n"
            "Envoie maintenant le fichier d’import :\n"
            "- .txt (format fourni), .csv, ou .jsonl\n"
            "Les gros fichiers peuvent être découpés.\n\n"
            "Quand l’import sera terminé, j’afficherai le menu de la base."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Retour", callback_data="home:db")]
        ])
        await message.answer(text, reply_markup=kb)
        return

    # Recherche par numéro
    if USER_STATE[user_id].get("awaiting_search_number"):
        USER_STATE[user_id]["awaiting_search_number"] = False
        await find_and_reply_number(message, message.text or "")
        return

    return  # autres textes ignorés

# ----------------- Créer une base (déclenche saisie nom) -----------------
@router.callback_query(F.data == "db:create")
async def db_create_start(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    USER_STATE[user_id]["awaiting_base_name"] = True
    text = ("Nouvelle base\n\n"
            "Envoie le nom de la base à créer.\n"
            "Autorisé: lettres, chiffres, underscore (_). Max 40.")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Retour", callback_data="home:db")]
    ])
    await show_page(cb, text, kb)

# ----------------- Statistiques (départements uniquement) -----------------
def sorted_dept_counts(counts: Dict[str,int]) -> List[Tuple[str,int]]:
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))

@router.callback_query(F.data.startswith("db:stats:"))
async def db_stats(cb: CallbackQuery):
    name = cb.data.split(":", 2)[2]
    meta = BASES.get(name)
    if not meta:
        await safe_cb_answer(cb, "Base introuvable.")
        return

    total = meta["records"]
    depts = sorted_dept_counts(meta.get("dept_counts", {}))
    if depts:
        lines = [f"Statistiques — {name}", "", f"Total fiches : {total}", ""]
        for code, n in depts:
            lines.append(f"- {code} : {n} fiche(s)")
        text = "\n".join(lines)
    else:
        text = f"Statistiques — {name}\n\nTotal fiches : {total}\n\nAucun département détecté."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Retour", callback_data=f"db:open:{name}")]
    ])
    await show_page(cb, text, kb)

# ----------------- Import -----------------
@router.callback_query(F.data.startswith("db:import:"))
async def db_import_start(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await safe_cb_answer(cb, "Base introuvable.")
        return
    ensure_user(user_id)
    set_active_db(user_id, name)
    USER_STATE[user_id]["awaiting_import_for_base"] = name

    text = (
        f"Import dans la base « {name} ».\n\n"
        "Envoie un fichier .txt (format fourni), .csv ou .jsonl.\n"
        "Les fichiers volumineux peuvent être découpés."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Retour", callback_data=f"db:open:{name}")]
    ])
    await show_page(cb, text, kb)

@router.message(F.document)
async def handle_import_file(message: Message):
    user_id = message.from_user.id
    ensure_user(user_id)
    target = USER_STATE[user_id].get("awaiting_import_for_base")
    if not target:
        return

    filename = message.document.file_name or ""
    allowed = (filename.endswith(".csv") or filename.endswith(".json")
               or filename.endswith(".jsonl") or filename.endswith(".txt"))
    if not allowed:
        await message.answer("Format non pris en charge. Envoie .csv, .json, .jsonl ou .txt.")
        return

    tg_file = await bot.get_file(message.document.file_id)
    dst_path = f"/tmp/{message.document.file_unique_id}_{filename}"
    await bot.download(tg_file, destination=dst_path)

    added_records = 0
    added_phone_count = 0
    size_mb = round((os.path.getsize(dst_path) / (1024 * 1024)), 2)

    try:
        if filename.endswith(".txt"):
            with open(dst_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            records = parse_txt_blocks(content)

            for r in records:
                dept = dept_from_cp(r.get("cp"))
                r["dept"] = dept
                r.setdefault("notes", [])
                # rid avant append
                r["rid"] = str(len(BASES[target]["records_list"]))
                BASES[target]["records_list"].append(r)
                added_records += 1
                if r.get("mobile"):
                    added_phone_count += 1
                if r.get("voip"):
                    added_phone_count += 1
                if dept:
                    BASES[target]["dept_counts"][dept] = BASES[target]["dept_counts"].get(dept, 0) + 1

        elif filename.endswith(".csv"):
            # TODO: parser CSV réel si besoin
            pass

        elif filename.endswith(".jsonl") or filename.endswith(".json"):
            # TODO: parser JSON/JSONL réel si besoin
            pass

    except Exception as e:
        USER_STATE[user_id]["awaiting_import_for_base"] = None
        await message.answer(f"Erreur pendant l'import: {e}")
        return

    BASES[target]["records"] += added_records
    BASES[target]["phone_count"] = BASES[target].get("phone_count", 0) + added_phone_count
    BASES[target]["size_mb"] = round(BASES[target]["size_mb"] + size_mb, 2)
    BASES[target]["last_import"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    USER_STATE[user_id]["awaiting_import_for_base"] = None

    await message.answer(
        f"Import terminé dans « {target} ».\n"
        f"- Fiches ajoutées: {added_records}\n"
        f"- Taille du fichier: {size_mb} Mo\n"
        f"- Total fiches: {BASES[target]['records']}"
    )

    text = f"Base sélectionnée : {target}\n\nChoisissez une action."
    kb = base_menu_keyboard(target)
    await message.answer(text, reply_markup=kb)

# ----------------- Export CSV -----------------
@router.callback_query(F.data.startswith("db:export:"))
async def db_export(cb: CallbackQuery):
    name = cb.data.split(":", 2)[2]
    meta = BASES.get(name)
    if not meta:
        await safe_cb_answer(cb, "Base introuvable.")
        return

    headers = ["rid", "last_name", "first_name", "full_name_raw", "email", "mobile", "voip",
               "ville", "cp", "dept", "adresse", "iban", "bic", "dob", "statut", "notes"]
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for rec in meta.get("records_list", []):
        row = dict(rec)
        if isinstance(row.get("notes"), list):
            row["notes"] = " | ".join(row["notes"])
        writer.writerow(row)
    csv_data = buf.getvalue().encode("utf-8")

    tmp_path = f"/tmp/export_{name}_{int(datetime.now().timestamp())}.csv"
    with open(tmp_path, "wb") as f:
        f.write(csv_data)

    await cb.message.answer_document(
        document=FSInputFile(tmp_path, filename=f"{name}.csv"),
        caption=f"Export CSV — {name} ({len(meta.get('records_list', []))} fiches)."
    )
    await safe_cb_answer(cb)

# ----------------- Supprimer (depuis le menu de la base) -----------------
@router.callback_query(F.data.startswith("db:drop:"))
async def db_drop(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await safe_cb_answer(cb, "Base introuvable.")
        return
    if get_active_db(user_id) != name:
        await safe_cb_answer(cb, "Sélectionne d’abord la base, puis supprime.")
        return

    text = f"Confirmer la suppression de la base « {name} » ? Action définitive."
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Supprimer définitivement", callback_data=f"db:dropconfirm:{name}")],
        [InlineKeyboardButton(text="Retour", callback_data=f"db:open:{name}")]
    ])
    await show_page(cb, text, kb)

@router.callback_query(F.data.startswith("db:dropconfirm:"))
async def db_drop_confirm(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await safe_cb_answer(cb, "Base introuvable.")
        return
    if get_active_db(user_id) != name:
        await safe_cb_answer(cb, "Sélectionne d’abord la base, puis supprime.")
        return
    if len(BASES) == 1:
        await safe_cb_answer(cb, "Impossible: il doit rester au moins une base.")
        return

    del BASES[name]
    set_active_db(user_id, "default" if "default" in BASES else next(iter(BASES.keys())))

    text = render_db_list_text_only()
    kb = db_list_keyboard(user_id)
    await show_page(cb, text, kb)

# ----------------- Actions fiche: En ligne / Fin d’appel / Non traité / Note -----------------
@router.callback_query(F.data.startswith("rec:ongoing:"))
async def rec_mark_ongoing(cb: CallbackQuery):
    # rec:ongoing:<base>:<rid>
    try:
        _, _, base, rid = cb.data.split(":", 3)
    except Exception:
        return await safe_cb_answer(cb)
    user_id = cb.from_user.id
    ensure_user(user_id)
    set_active_db(user_id, base)
    ensure_record_ids(base)

    lst = USER_INPROGRESS[user_id].setdefault(base, [])
    if rid not in lst:
        lst.append(rid)
        inc_stat(user_id, "cases", 1)
    await safe_cb_answer(cb, "📞 Mis en 'Dossiers en cours'.")

@router.callback_query(F.data.startswith("rec:finish:"))
async def rec_finish_call(cb: CallbackQuery):
    # rec:finish:<base>:<rid>
    try:
        _, _, base, rid = cb.data.split(":", 3)
    except Exception:
        return await safe_cb_answer(cb)
    user_id = cb.from_user.id
    ensure_user(user_id)
    set_active_db(user_id, base)
    ensure_record_ids(base)

    # Retire des en-cours (si présent) -> décrémente cases
    inprog = USER_INPROGRESS[user_id].setdefault(base, [])
    if rid in inprog:
        inprog.remove(rid)
        inc_stat(user_id, "cases", -1)

    # Ajoute aux traités (si nouveau) -> incrémente treated
    treated = USER_TREATED[user_id].setdefault(base, [])
    if rid not in treated:
        treated.append(rid)
        inc_stat(user_id, "treated", 1)

    await safe_cb_answer(cb, "🟢 Fin d’appel — classé en 'traités'.")

@router.callback_query(F.data.startswith("rec:missed:"))
async def rec_mark_missed(cb: CallbackQuery):
    # rec:missed:<base>:<rid>
    try:
        _, _, base, rid = cb.data.split(":", 3)
    except Exception:
        return await safe_cb_answer(cb)
    user_id = cb.from_user.id
    ensure_user(user_id)
    set_active_db(user_id, base)
    ensure_record_ids(base)

    # Sort des en-cours si présent
    inprog = USER_INPROGRESS[user_id].setdefault(base, [])
    if rid in inprog:
        inprog.remove(rid)
        inc_stat(user_id, "cases", -1)

    # Ajoute aux manqués
    missed = USER_MISSED[user_id].setdefault(base, [])
    if rid not in missed:
        missed.append(rid)
        inc_stat(user_id, "missed", 1)

    await safe_cb_answer(cb, "❌ Marqué comme non traité.")

# ----------------- Ajouter une note -----------------
@router.callback_query(F.data.startswith("rec:note:"))
async def rec_note_start(cb: CallbackQuery):
    # rec:note:<base>:<rid>
    try:
        _, _, base, rid = cb.data.split(":", 3)
    except Exception:
        return await safe_cb_answer(cb)
    user_id = cb.from_user.id
    ensure_user(user_id)
    rec = find_record(base, rid)
    if not rec:
        return await safe_cb_answer(cb, "Fiche introuvable.")
    USER_STATE[user_id]["awaiting_note_for"] = {
        "base": base,
        "rid": rid,
        "chat_id": cb.message.chat.id,
        "message_id": cb.message.message_id
    }
    await safe_cb_answer(cb, "Envoie maintenant le texte de la note.")

# ----------------- Listes: traités / en cours -----------------
@router.callback_query(F.data == "home:treated")
async def list_treated(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    base = get_active_db(user_id)
    ensure_record_ids(base)
    treated = USER_TREATED[user_id].get(base, []) or []
    text = f"Clients traités — base {base}\n\n"
    if not treated:
        text += "Aucun client traité pour le moment."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Retour", callback_data="nav:start")]
        ])
        return await show_page(cb, text, kb)

    rows = []
    for rid in treated[-50:][::-1]:  # derniers d'abord
        rec = find_record(base, rid)
        if not rec:
            continue
        label = f"{pretty_name(rec)} — {rec.get('ville') or '—'} ({rec.get('cp') or '—'})"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"rec:view:{base}:{rid}")])
    rows.append([InlineKeyboardButton(text="Retour", callback_data="nav:start")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await show_page(cb, text.strip(), kb)

@router.callback_query(F.data == "home:cases")
async def list_cases(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    base = get_active_db(user_id)
    ensure_record_ids(base)
    inprog = USER_INPROGRESS[user_id].get(base, []) or []
    text = f"Dossiers en cours — base {base}\n\n"
    if not inprog:
        text += "Aucun dossier en cours."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Retour", callback_data="nav:start")]
        ])
        return await show_page(cb, text, kb)

    rows = []
    for rid in inprog[-50:][::-1]:
        rec = find_record(base, rid)
        if not rec:
            continue
        label = f"{pretty_name(rec)} — {rec.get('ville') or '—'} ({rec.get('cp') or '—'})"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"rec:view:{base}:{rid}")])
    rows.append([InlineKeyboardButton(text="Retour", callback_data="nav:start")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await show_page(cb, text.strip(), kb)

# ----------------- Retour accueil -----------------
@router.callback_query(F.data == "nav:start")
async def back_to_start(cb: CallbackQuery):
    ensure_user(cb.from_user.id)
    USER_STATE[cb.from_user.id]["awaiting_search_number"] = False

    active_db = get_active_db(cb.from_user.id)
    stats = get_today_stats(cb.from_user.id)
    nb_contactes = stats.get("treated", 0)
    nb_appels_manques = stats.get("missed", 0)
    nb_dossiers_en_cours_jour = stats.get("cases", 0)
    nb_fiches = BASES.get(active_db, {}).get("records", 0)

    treated_count = len(USER_TREATED.get(cb.from_user.id, {}).get(active_db, []))
    inprogress_count = len(USER_INPROGRESS.get(cb.from_user.id, {}).get(active_db, []))

    text = (
        "👋 Bienvenue sur FICHES CLIENTS\n\n"
        f"Base active : {active_db}\n\n"
        "Statistiques du jour :\n"
        f"- Clients traités : {nb_contactes}\n"
        f"- Appels manqués : {nb_appels_manques}\n"
        f"- Dossiers en cours : {nb_dossiers_en_cours_jour}\n"
        f"- Fiches totales : {nb_fiches}\n\n"
        "Utilisez les boutons ci-dessous ou tapez /start pour revenir à l'accueil."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗄️ Gérer les bases", callback_data="home:db")],
        [InlineKeyboardButton(text="🔎 Rechercher une fiche", callback_data="home:search")],
        [InlineKeyboardButton(text=f"✅ Clients traités ({treated_count})", callback_data="home:treated")],
        [InlineKeyboardButton(text=f"🗂️ Dossiers en cours ({inprogress_count})", callback_data="home:cases")],
        [InlineKeyboardButton(text=f"📵 Appels manqués ({nb_appels_manques})", callback_data="home:missed")],
    ])
    await show_page(cb, text, kb, photo_url="https://i.postimg.cc/0jNN08J5/IMG-0294.jpg")

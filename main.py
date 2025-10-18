# main.py — FastAPI + aiogram v3.x (Render webhook)
import os
import re
import csv
import asyncio
import uuid
from io import StringIO
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone, timedelta
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

TZ = ZoneInfo("Europe/Paris")  # pour RDV & stats
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
# flags:
# - awaiting_base_name: bool
# - awaiting_import_for_base: str|None
# - awaiting_search_number: bool
# - awaiting_note_for: {"base","rid","chat_id","message_id"}|None
# - awaiting_rdv_for: {"base","rid","chat_id","message_id"}|None

# Stats du jour par utilisateur
USER_DAILY_STATS: Dict[int, Dict[str, Dict[str, int]]] = {}

# Listes des fiches marquées
USER_TREATED: Dict[int, Dict[str, List[str]]] = {}
USER_MISSED: Dict[int, Dict[str, List[str]]] = {}
USER_INPROGRESS: Dict[int, Dict[str, List[str]]] = {}

# RDV: USER_RDV[user_id][base] = [{"id","rid","at_iso","remind_iso","sent","chat_id"}]
USER_RDV: Dict[int, Dict[str, List[Dict]]] = {}

def ensure_user(user_id: int) -> None:
    USER_PREFS.setdefault(user_id, {"active_db": "default"})
    USER_STATE.setdefault(user_id, {})
    USER_DAILY_STATS.setdefault(user_id, {})
    USER_TREATED.setdefault(user_id, {})
    USER_MISSED.setdefault(user_id, {})
    USER_INPROGRESS.setdefault(user_id, {})
    USER_RDV.setdefault(user_id, {})

def get_active_db(user_id: int) -> str:
    return USER_PREFS.get(user_id, {}).get("active_db", "default")

def set_active_db(user_id: int, dbname: str) -> None:
    USER_PREFS.setdefault(user_id, {})["active_db"] = dbname
    USER_TREATED[user_id].setdefault(dbname, [])
    USER_MISSED[user_id].setdefault(dbname, [])
    USER_INPROGRESS[user_id].setdefault(dbname, [])
    USER_RDV[user_id].setdefault(dbname, [])

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
        "notes": [],
        "next_rdv_iso": None
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
    base = BASES.get(base_name, {})
    lst = base.get("records_list", [])
    for idx, r in enumerate(lst):
        if not r.get("rid"):
            r["rid"] = str(idx)

# Une seule page à la fois (navigation)
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

def format_dt_short(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%d/%m %H:%M")

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
        lines = [f"- {n}" for n in notes_list[-10:]]
        notes_block = "\n\nNotes :\n" + "\n".join(lines)
    rdv_block = ""
    if rec.get("next_rdv_iso"):
        try:
            dt = datetime.fromisoformat(rec["next_rdv_iso"])
            rdv_block = f"\n- RDV : {format_dt_short(dt)}"
        except Exception:
            pass
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
        f"{rdv_block}"
        f"{notes_block}"
    )

def record_keyboard(base: str, rid: str, rec: Optional[Dict] = None) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📞 En ligne",         callback_data=f"rec:ongoing:{base}:{rid}")],
        [InlineKeyboardButton(text="🟢 Fin d’appel",      callback_data=f"rec:finish:{base}:{rid}")],
        [InlineKeyboardButton(text="❌ Non traité",       callback_data=f"rec:missed:{base}:{rid}")],
        [InlineKeyboardButton(text="📝 Ajouter une note", callback_data=f"rec:note:{base}:{rid}")],
        [InlineKeyboardButton(text="📅 Placer un RDV",    callback_data=f"rec:rdv:{base}:{rid}")],
    ]
    if rec and rec.get("next_rdv_iso"):
        rows.append([InlineKeyboardButton(text="🗑️ Annuler RDV", callback_data=f"rec:rdv_cancel:{base}:{rid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def send_record_card(chat_id: int, base: str, rec: Dict):
    text = render_record_text(rec)
    kb = record_keyboard(base, rec.get("rid", "0"), rec)
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
    nb_appels_manques_day = stats.get("missed", 0)  # stats (jour)
    nb_dossiers_en_cours_day = stats.get("cases", 0)
    nb_fiches = BASES.get(active_db, {}).get("records", 0)

    treated_count = len(USER_TREATED.get(user_id, {}).get(active_db, []))
    inprogress_count = len(USER_INPROGRESS.get(user_id, {}).get(active_db, []))
    missed_count = len(USER_MISSED.get(user_id, {}).get(active_db, []))
    rdv_count = len([r for r in USER_RDV.get(user_id, {}).get(active_db, []) if not r.get("sent") and datetime.fromisoformat(r["at_iso"]) >= datetime.now(TZ)])

    text = (
        f"Base active : {active_db}\n\n"
        "Statistiques du jour :\n"
        f"- Clients traités : {nb_contactes}\n"
        f"- Appels manqués : {nb_appels_manques_day}\n"
        f"- Dossiers en cours : {nb_dossiers_en_cours_day}\n"
        f"- Fiches totales : {nb_fiches}\n\n"
        "Utilisez les boutons ci-dessous ou tapez /start pour revenir à l'accueil."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗄️ Gérer les bases", callback_data="home:db")],
        [InlineKeyboardButton(text="🔎 Rechercher une fiche", callback_data="home:search")],
        [InlineKeyboardButton(text=f"✅ Clients traités ({treated_count})", callback_data="home:treated")],
        [InlineKeyboardButton(text=f"🗂️ Dossiers en cours ({inprogress_count})", callback_data="home:cases")],
        [InlineKeyboardButton(text=f"📵 Appels manqués ({missed_count})", callback_data="home:missed")],
        [InlineKeyboardButton(text=f"📅 RDV programmés ({rdv_count})", callback_data="home:rdv")],
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

# ----------------- Voir fiche via bouton -----------------
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

# ----------------- Saisies texte : nom / recherche / note / rdv -----------------
def parse_time_fr(s: str) -> Optional[Tuple[int, int]]:
    s = (s or "").strip().lower().replace(" ", "")
    # 16h30 | 16:30 | 1630 | 16h
    m = re.match(r"^(\d{1,2})h?[:]?(\d{2})?$", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2) or "00")
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return hh, mm

@router.message(F.text)
async def capture_text(message: Message):
    user_id = message.from_user.id
    ensure_user(user_id)

    # ---- Note en attente
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
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=render_record_text(rec),
                reply_markup=record_keyboard(base, rid, rec)
            )
        except Exception:
            await send_record_card(message.chat.id, base, rec)
        await message.answer("✅ Note ajoutée.")
        return

    # ---- RDV en attente
    rdv_target = USER_STATE[user_id].get("awaiting_rdv_for")
    if rdv_target:
        base = rdv_target["base"]; rid = rdv_target["rid"]
        chat_id = rdv_target["chat_id"]; msg_id = rdv_target["message_id"]
        USER_STATE[user_id]["awaiting_rdv_for"] = None
        hm = parse_time_fr(message.text or "")
        if not hm:
            await message.answer("Heure invalide. Exemples : 16h30, 16:30, 1630, 16h")
            return
        h, m = hm
        now = datetime.now(TZ)
        at = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if at <= now:
            at = at + timedelta(days=1)  # si passé, demain
        remind = at - timedelta(minutes=5)

        rdv_id = uuid.uuid4().hex
        USER_RDV[user_id].setdefault(base, []).append({
            "id": rdv_id,
            "rid": rid,
            "at_iso": at.isoformat(),
            "remind_iso": remind.isoformat(),
            "sent": False,
            "chat_id": message.chat.id
        })

        rec = find_record(base, rid)
        if rec:
            # met à jour prochain RDV
            rec["next_rdv_iso"] = at.isoformat()
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=render_record_text(rec),
                    reply_markup=record_keyboard(base, rid, rec)
                )
            except Exception:
                await send_record_card(message.chat.id, base, rec)

        await message.answer(f"📅 RDV placé pour {format_dt_short(at)} (rappel 5 min avant).")
        return

    # ---- Création base : on attend un nom
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

    # ---- Recherche par numéro
    if USER_STATE[user_id].get("awaiting_search_number"):
        USER_STATE[user_id]["awaiting_search_number"] = False
        await find_and_reply_number(message, message.text or "")
        return

    return  # autres textes ignorés

# ----------------- Créer une base -----------------
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
                r.setdefault("next_rdv_iso", None)
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
               "ville", "cp", "dept", "adresse", "iban", "bic", "dob", "statut", "notes", "next_rdv_iso"]
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

# ----------------- Actions fiche -----------------
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

    inprog = USER_INPROGRESS[user_id].setdefault(base, [])
    if rid not in inprog:
        inprog.append(rid)
        inc_stat(user_id, "cases", 1)

    missed = USER_MISSED[user_id].setdefault(base, [])
    if rid in missed:
        missed.remove(rid)

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

    inprog = USER_INPROGRESS[user_id].setdefault(base, [])
    if rid in inprog:
        inprog.remove(rid)
        inc_stat(user_id, "cases", -1)

    missed = USER_MISSED[user_id].setdefault(base, [])
    if rid in missed:
        missed.remove(rid)

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

    inprog = USER_INPROGRESS[user_id].setdefault(base, [])
    if rid in inprog:
        inprog.remove(rid)
        inc_stat(user_id, "cases", -1)

    missed = USER_MISSED[user_id].setdefault(base, [])
    if rid not in missed:
        missed.append(rid)
        inc_stat(user_id, "missed", 1)

    await safe_cb_answer(cb, "❌ Marqué comme non traité (Appels manqués).")

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

# ----------------- Helpers RDV -----------------
def get_upcoming_rdvs(user_id: int, base: str, rid: Optional[str] = None):
    """Liste des RDV futurs non envoyés, triés par date. Si rid=None, tous."""
    now = datetime.now(TZ)
    out = []
    for it in USER_RDV.get(user_id, {}).get(base, []):
        if it.get("sent"):
            continue
        if rid and it.get("rid") != rid:
            continue
        try:
            at = datetime.fromisoformat(it["at_iso"])
        except Exception:
            continue
        if at >= now:
            out.append((at, it))
    out.sort(key=lambda x: x[0])
    return out

def _refresh_record_next_rdv(user_id: int, base: str, rid: str):
    rec = find_record(base, rid)
    if not rec:
        return
    upcoming = get_upcoming_rdvs(user_id, base, rid)
    if upcoming:
        rec["next_rdv_iso"] = upcoming[0][0].isoformat()
    else:
        rec["next_rdv_iso"] = None

def _cancel_rdv_by_id(user_id: int, base: str, rdv_id: str) -> bool:
    lst = USER_RDV.get(user_id, {}).get(base, [])
    if not lst:
        return False
    kept = []
    cancelled = False
    target_rid = None
    for it in lst:
        if it.get("id") == rdv_id and not it.get("sent"):
            cancelled = True
            target_rid = it.get("rid")
            continue
        kept.append(it)
    if cancelled:
        USER_RDV[user_id][base] = kept
        if target_rid:
            _refresh_record_next_rdv(user_id, base, target_rid)
    return cancelled

# ----------------- Placer / Lister / Annuler un RDV -----------------
@router.callback_query(F.data.startswith("rec:rdv:"))
async def rec_rdv_start(cb: CallbackQuery):
    # rec:rdv:<base>:<rid>
    try:
        _, _, base, rid = cb.data.split(":", 3)
    except Exception:
        return await safe_cb_answer(cb)
    user_id = cb.from_user.id
    ensure_user(user_id)
    rec = find_record(base, rid)
    if not rec:
        return await safe_cb_answer(cb, "Fiche introuvable.")

    USER_STATE[user_id]["awaiting_rdv_for"] = {
        "base": base,
        "rid": rid,
        "chat_id": cb.message.chat.id,
        "message_id": cb.message.message_id
    }
    await safe_cb_answer(cb, "Envoie l’heure du RDV (ex: 16h30, 16:30, 1630).")

@router.callback_query(F.data.startswith("rec:rdv_cancel:"))
async def rec_rdv_cancel(cb: CallbackQuery):
    # rec:rdv_cancel:<base>:<rid>  -> Affiche la liste des RDV futurs pour cette fiche
    try:
        _, _, base, rid = cb.data.split(":", 3)
    except Exception:
        return await safe_cb_answer(cb)
    user_id = cb.from_user.id
    ensure_user(user_id)

    upcoming = get_upcoming_rdvs(user_id, base, rid)
    if not upcoming:
        return await safe_cb_answer(cb, "Aucun RDV futur à annuler pour cette fiche.")

    text = "RDV de cette fiche (sélectionne à annuler) :"
    rows = []
    for at, it in upcoming[:25]:
        label = f"🗑️ {format_dt_short(at)}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"rdv:cancel_one:{base}:{it['rid']}:{it['id']}")])
    rows.append([InlineKeyboardButton(text="Retour fiche", callback_data=f"rec:view:{base}:{rid}")])
    await show_page(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))

@router.callback_query(F.data.startswith("rdv:cancel_one:"))
async def rdv_cancel_one(cb: CallbackQuery):
    # rdv:cancel_one:<base>:<rid>:<rdv_id>
    try:
        _, _, base, rid, rdv_id = cb.data.split(":", 4)
    except Exception:
        return await safe_cb_answer(cb)
    user_id = cb.from_user.id
    ensure_user(user_id)
    ok = _cancel_rdv_by_id(user_id, base, rdv_id)
    await safe_cb_answer(cb, "🗑️ RDV annulé." if ok else "RDV introuvable ou déjà passé.")
    # Retour à la fiche (rafraîchie)
    rec = find_record(base, rid)
    if rec:
        await send_record_card(cb.message.chat.id, base, rec)

@router.callback_query(F.data == "home:rdv")
async def list_rdv(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    base = get_active_db(user_id)
    upcoming = get_upcoming_rdvs(user_id, base, rid=None)
    text = f"RDV programmés — base {base}\n\n"
    if not upcoming:
        text += "Aucun RDV à venir."
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Retour", callback_data="nav:start")]])
        return await show_page(cb, text, kb)
    rows = []
    for at, it in upcoming[:50]:
        rec = find_record(base, it["rid"])
        who = f"{pretty_name(rec) if rec else 'Fiche'}"
        # 2 boutons : ouvrir + annuler ce RDV précisément
        rows.append([
            InlineKeyboardButton(text=f"⏱ {format_dt_short(at)} — {who}", callback_data=f"rec:view:{base}:{it['rid']}"),
            InlineKeyboardButton(text="🗑️ Annuler", callback_data=f"rdv:cancel_one:{base}:{it['rid']}:{it['id']}")
        ])
    rows.append([InlineKeyboardButton(text="Retour", callback_data="nav:start")])
    await show_page(cb, text.strip(), InlineKeyboardMarkup(inline_keyboard=rows))

# ----------------- Retour accueil -----------------
@router.callback_query(F.data == "nav:start")
async def back_to_start(cb: CallbackQuery):
    ensure_user(cb.from_user.id)
    USER_STATE[cb.from_user.id]["awaiting_search_number"] = False

    active_db = get_active_db(cb.from_user.id)
    stats = get_today_stats(cb.from_user.id)
    nb_contactes = stats.get("treated", 0)
    nb_appels_manques_day = stats.get("missed", 0)
    nb_dossiers_en_cours_day = stats.get("cases", 0)
    nb_fiches = BASES.get(active_db, {}).get("records", 0)

    treated_count = len(USER_TREATED.get(cb.from_user.id, {}).get(active_db, []))
    inprogress_count = len(USER_INPROGRESS.get(cb.from_user.id, {}).get(active_db, []))
    missed_count = len(USER_MISSED.get(cb.from_user.id, {}).get(active_db, []))
    rdv_count = len([r for r in USER_RDV.get(cb.from_user.id, {}).get(active_db, []) if not r.get("sent") and datetime.fromisoformat(r["at_iso"]) >= datetime.now(TZ)])

    text = (
        "👋 Bienvenue sur FICHES CLIENTS\n\n"
        f"Base active : {active_db}\n\n"
        "Statistiques du jour :\n"
        f"- Clients traités : {nb_contactes}\n"
        f"- Appels manqués : {nb_appels_manques_day}\n"
        f"- Dossiers en cours : {nb_dossiers_en_cours_day}\n"
        f"- Fiches totales : {nb_fiches}\n\n"
        "Utilisez les boutons ci-dessous ou tapez /start pour revenir à l'accueil."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗄️ Gérer les bases", callback_data="home:db")],
        [InlineKeyboardButton(text="🔎 Rechercher une fiche", callback_data="home:search")],
        [InlineKeyboardButton(text=f"✅ Clients traités ({treated_count})", callback_data="home:treated")],
        [InlineKeyboardButton(text=f"🗂️ Dossiers en cours ({inprogress_count})", callback_data="home:cases")],
        [InlineKeyboardButton(text=f"📵 Appels manqués ({missed_count})", callback_data="home:missed")],
        [InlineKeyboardButton(text=f"📅 RDV programmés ({rdv_count})", callback_data="home:rdv")],
    ])
    await show_page(cb, text, kb, photo_url="https://i.postimg.cc/0jNN08J5/IMG-0294.jpg")

# ----------------- Scheduler RDV (rappel -5 min) -----------------
async def rdv_scheduler():
    while True:
        try:
            now = datetime.now(TZ)
            for user_id, bases in list(USER_RDV.items()):
                for base, items in list(bases.items()):
                    for it in items:
                        if it.get("sent"):
                            continue
                        try:
                            remind_at = datetime.fromisoformat(it["remind_iso"])
                        except Exception:
                            continue
                        if now >= remind_at:
                            rid = it["rid"]
                            rec = find_record(base, rid)
                            name = pretty_name(rec) if rec else f"Fiche {rid}"
                            at = datetime.fromisoformat(it["at_iso"]).astimezone(TZ).strftime("%H:%M")
                            try:
                                await bot.send_message(
                                    chat_id=it["chat_id"],
                                    text=f"⏰ Rappel RDV à {at} avec {name}",
                                    reply_markup=InlineKeyboardMarkup(
                                        inline_keyboard=[[InlineKeyboardButton(text="Ouvrir la fiche", callback_data=f"rec:view:{base}:{rid}")]]
                                    )
                                )
                            except Exception:
                                pass
                            it["sent"] = True
            await asyncio.sleep(30)
        except Exception:
            await asyncio.sleep(30)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(rdv_scheduler())

# ----------------- Voir Clients traités / Dossiers en cours / Appels manqués -----------------

async def show_records_list(cb: CallbackQuery, title: str, rec_ids: List[str], base: str):
    if not rec_ids:
        text = f"Aucun {title.lower()} pour le moment."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Retour", callback_data="nav:start")]
        ])
        return await show_page(cb, text, kb)

    rows = []
    for rid in rec_ids[:50]:
        rec = find_record(base, rid)
        if not rec:
            continue
        name = pretty_name(rec)
        ville = rec.get("ville") or "—"
        cp = rec.get("cp") or "—"
        label = f"{name} — {ville} ({cp})"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"rec:view:{base}:{rid}")])

    rows.append([InlineKeyboardButton(text="Retour", callback_data="nav:start")])
    text = f"{title} — {len(rec_ids)} fiche(s) :"
    await show_page(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data == "home:treated")
async def show_treated(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    base = get_active_db(user_id)
    rec_ids = USER_TREATED.get(user_id, {}).get(base, [])
    await show_records_list(cb, "Clients traités", rec_ids, base)


@router.callback_query(F.data == "home:cases")
async def show_cases(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    base = get_active_db(user_id)
    rec_ids = USER_INPROGRESS.get(user_id, {}).get(base, [])
    await show_records_list(cb, "Dossiers en cours", rec_ids, base)


@router.callback_query(F.data == "home:missed")
async def show_missed(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    base = get_active_db(user_id)
    rec_ids = USER_MISSED.get(user_id, {}).get(base, [])
    await show_records_list(cb, "Appels manqués", rec_ids, base)

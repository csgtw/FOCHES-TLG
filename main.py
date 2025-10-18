# main.py — FastAPI + aiogram v3.x (compatible Render webhook)
import os
import re
import csv
from io import StringIO
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import CommandStart
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Update,
    CallbackQuery, Message, FSInputFile
)

# ----------------- Configuration -----------------
TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    raise RuntimeError("Missing TELEGRAM_TOKEN environment variable")

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()

# ----------------- FastAPI app -----------------
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

# -------------------------------------------------
# Stockage en mémoire (à remplacer par PostgreSQL plus tard)
# -------------------------------------------------
# BASES[name] = {
#   "records": int,
#   "size_mb": float,
#   "last_import": ISO str | None,
#   "phone_count": int,
#   "records_list": List[Dict],  # fiches
#   "dept_counts": Dict[str,int] # ex: {"31": 120, "75": 98, "971": 14}
# }
BASES: Dict[str, Dict] = {
    "default": {
        "records": 0, "size_mb": 0.0, "last_import": None, "phone_count": 0,
        "records_list": [], "dept_counts": {}
    }
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

# ----------------- Utilitaires parsing & stats -----------------
def normalize_phone(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    s = re.sub(r"^(?:\+33|0033)\s*", "0", s)  # +33/0033 -> 0
    digits = re.sub(r"\D", "", s)
    if len(digits) == 9:
        digits = "0" + digits
    return digits or None

def dept_from_cp(cp: Optional[str]) -> Optional[str]:
    if not cp or not re.fullmatch(r"\d{5}", cp):
        return None
    # Outre-mer: 97x / 98x (ex: 971xx, 972xx, 973xx, 974xx, 976xx, 986/987/988/989 ont souvent 98x…)
    if cp.startswith(("97", "98")):
        return cp[:3]
    return cp[:2]

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

# ----------------- Accueil factorisé & /start -----------------
async def send_home(chat_id: int, user_id: int):
    active_db = get_active_db(user_id)
    nb_contactes = 0          # à brancher plus tard
    nb_appels_manques = 0     # à brancher plus tard
    nb_dossiers_en_cours = 0  # à brancher plus tard
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
    await bot.send_photo(chat_id=chat_id, photo=image_url, caption=text,
                         parse_mode="Markdown", reply_markup=kb)

@router.message(CommandStart())
async def accueil(message: types.Message):
    user_id = message.from_user.id
    ensure_user(user_id)
    await send_home(chat_id=message.chat.id, user_id=user_id)

dp.include_router(router)

# ==========================================================
# GÉRER LES BASES + IMPORT + EXPORT + STATS PAR DÉPARTEMENT
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
        # bouton de sélection (active la base)
        rows.append([InlineKeyboardButton(text=f"{'●' if name == active else '○'} {name}",
                                          callback_data=f"db:use:{name}")])
        # rangée d'actions
        action_row = [
            InlineKeyboardButton(text="Stats", callback_data=f"db:stats:{name}"),
            InlineKeyboardButton(text="Importer", callback_data=f"db:import:{name}"),
            InlineKeyboardButton(text="Exporter", callback_data=f"db:export:{name}"),
        ]
        # le bouton "Supprimer" n'apparait QUE pour la base active
        if name == active:
            action_row.append(InlineKeyboardButton(text="Supprimer", callback_data=f"db:drop:{name}"))
        rows.append(action_row)
    rows.append([InlineKeyboardButton(text="Ajouter une base", callback_data="db:create")])
    rows.append([InlineKeyboardButton(text="Retour à l'accueil (/start)", callback_data="nav:start")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def edit_home_like(cb: CallbackQuery, text: str, kb: InlineKeyboardMarkup):
    # 1) essayer d’éditer la légende (si message photo)
    try:
        await cb.message.edit_caption(caption=text, reply_markup=kb)
        return
    except Exception:
        pass
    # 2) sinon essayer d’éditer le texte
    try:
        await cb.message.edit_text(text, reply_markup=kb)
        return
    except Exception:
        pass
    # 3) sinon envoyer un nouveau message
    await cb.message.answer(text, reply_markup=kb)

# --- Ouvrir "Gérer les bases" ---
@router.callback_query(F.data == "home:db")
async def open_db_list(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    text = render_db_list_text(user_id)
    kb = db_list_keyboard(user_id)
    await edit_home_like(cb, text, kb)
    await cb.answer()

# --- Utiliser une base ---
@router.callback_query(F.data.startswith("db:use:"))
async def db_use(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await cb.answer("Base introuvable.", show_alert=True)
        return
    set_active_db(user_id, name)
    text = render_db_list_text(user_id)
    kb = db_list_keyboard(user_id)
    await edit_home_like(cb, text, kb)
    await cb.answer(f"Base active: {name}")

# --- Stats d’une base (avec départements triés) ---
def sorted_dept_counts(counts: Dict[str,int]) -> List[Tuple[str,int]]:
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))

@router.callback_query(F.data.startswith("db:stats:"))
async def db_stats(cb: CallbackQuery):
    name = cb.data.split(":", 2)[2]
    meta = BASES.get(name)
    if not meta:
        await cb.answer("Base introuvable.", show_alert=True)
        return

    depts = sorted_dept_counts(meta.get("dept_counts", {}))
    # Construire un tableau texte simple
    if depts:
        dept_lines = ["Départements (triés) :", ""]
        for code, n in depts:
            dept_lines.append(f"- {code} : {n} fiche(s)")
        dept_text = "\n".join(dept_lines)
    else:
        dept_text = "Aucun code postal détecté pour cette base."

    text = (
        f"Statistiques de la base: {name}\n\n"
        f"- Fiches: {meta['records']}\n"
        f"- Numéros: {meta.get('phone_count', 0)}\n"
        f"- Taille estimée: {meta['size_mb']} Mo\n"
        f"- Dernier import: {meta['last_import'] or '—'}\n\n"
        f"{dept_text}\n\n"
        "Actions disponibles:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Importer", callback_data=f"db:import:{name}"),
         InlineKeyboardButton(text="Exporter", callback_data=f"db:export:{name}")],
        [InlineKeyboardButton(text="Retour aux bases", callback_data="home:db")]
    ])
    await edit_home_like(cb, text, kb)
    await cb.answer()

# --- Créer une base (saisie nom) ---
@router.callback_query(F.data == "db:create")
async def db_create_start(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    USER_STATE[user_id]["awaiting_base_name"] = True
    text = ("Envoyez le nom de la nouvelle base.\n"
            "Caractères autorisés: lettres, chiffres, underscore (_).")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Annuler", callback_data="home:db")]
    ])
    await edit_home_like(cb, text, kb)
    await cb.answer()

@router.message()  # capture le nom de base si on l'attend
async def capture_base_name(message: Message):
    user_id = message.from_user.id
    ensure_user(user_id)
    if not USER_STATE[user_id].get("awaiting_base_name"):
        return

    raw = (message.text or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_]{1,40}", raw):
        await message.answer("Nom invalide. Autorisés: A–Z, a–z, 0–9, _. Longueur max 40.")
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

    text = render_db_list_text(user_id)
    kb = db_list_keyboard(user_id)
    await message.answer(text, reply_markup=kb)

# --- Supprimer une base (affiché seulement pour la base active) ---
@router.callback_query(F.data.startswith("db:drop:"))
async def db_drop(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await cb.answer("Base introuvable.", show_alert=True)
        return
    # vérif sécurité: on n'autorise la suppression que si c'est la base active de l'utilisateur
    if get_active_db(user_id) != name:
        await cb.answer("Sélectionne d'abord cette base pour la supprimer (elle doit être active).", show_alert=True)
        return

    text = f"Confirmer la suppression de la base « {name} » ? Cette action est définitive."
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Supprimer définitivement", callback_data=f"db:dropconfirm:{name}")],
        [InlineKeyboardButton(text="Annuler", callback_data="home:db")]
    ])
    await edit_home_like(cb, text, kb)
    await cb.answer()

@router.callback_query(F.data.startswith("db:dropconfirm:"))
async def db_drop_confirm(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await cb.answer("Base introuvable.", show_alert=True)
        return
    if get_active_db(user_id) != name:
        await cb.answer("Sélectionne d'abord cette base pour la supprimer (elle doit être active).", show_alert=True)
        return
    if len(BASES) == 1:
        await cb.answer("Impossible: il doit rester au moins une base.", show_alert=True)
        return

    del BASES[name]
    # fallback: bascule sur 'default' si possible, sinon n'importe laquelle
    set_active_db(user_id, "default" if "default" in BASES else next(iter(BASES.keys())))

    text = render_db_list_text(user_id)
    kb = db_list_keyboard(user_id)
    await edit_home_like(cb, text, kb)
    await cb.answer("Base supprimée.")

# --- Import: demande fichier ---
@router.callback_query(F.data.startswith("db:import:"))
async def db_import_start(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await cb.answer("Base introuvable.", show_alert=True)
        return
    ensure_user(user_id)
    USER_STATE[user_id]["awaiting_import_for_base"] = name
    text = (
        f"Import dans la base « {name} ».\n\n"
        "Envoie un fichier .csv, .json, .jsonl ou .txt en pièce jointe.\n"
        "Les fichiers volumineux peuvent être découpés."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Annuler", callback_data="home:db")]
    ])
    await edit_home_like(cb, text, kb)
    await cb.answer()

# --- Réception d'un document et import TXT (stockage réel en mémoire) ---
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

            # Stockage réel en mémoire + maj compteurs
            for r in records:
                # calcule, stocke le département
                dept = dept_from_cp(r.get("cp"))
                r["dept"] = dept
                BASES[target]["records_list"].append(r)
                added_records += 1
                if r.get("mobile"):
                    added_phone_count += 1
                if r.get("voip"):
                    added_phone_count += 1
                if dept:
                    BASES[target]["dept_counts"][dept] = BASES[target]["dept_counts"].get(dept, 0) + 1

        elif filename.endswith(".csv"):
            # TODO: parser CSV réel (ajouter dans records_list, phone_count, dept_counts)
            pass

        elif filename.endswith(".jsonl") or filename.endswith(".json"):
            # TODO: parser JSON/JSONL réel
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
        f"- Numéros ajoutés: {added_phone_count}\n"
        f"- Taille du fichier: {size_mb} Mo\n"
        f"- Total fiches: {BASES[target]['records']}\n"
        f"- Total numéros: {BASES[target]['phone_count']}"
    )

    text = render_db_list_text(user_id)
    kb = db_list_keyboard(user_id)
    await message.answer(text, reply_markup=kb)

# --- Export (réel): génère un CSV depuis records_list et l'envoie ---
@router.callback_query(F.data.startswith("db:export:"))
async def db_export(cb: CallbackQuery):
    name = cb.data.split(":", 2)[2]
    meta = BASES.get(name)
    if not meta:
        await cb.answer("Base introuvable.", show_alert=True)
        return

    # Génération CSV en mémoire
    headers = ["last_name", "first_name", "full_name_raw", "email", "mobile", "voip",
               "ville", "cp", "dept", "adresse", "iban", "bic", "dob", "statut"]
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for rec in meta.get("records_list", []):
        writer.writerow(rec)
    csv_data = buf.getvalue().encode("utf-8")

    # Écriture dans /tmp et envoi
    tmp_path = f"/tmp/export_{name}_{int(datetime.now().timestamp())}.csv"
    with open(tmp_path, "wb") as f:
        f.write(csv_data)

    await cb.message.answer_document(
        document=FSInputFile(tmp_path, filename=f"{name}.csv"),
        caption=f"Export CSV de la base « {name} » ({len(meta.get('records_list', []))} fiches)."
    )
    await cb.answer()

# --- Retour à l'accueil via bouton ---
@router.callback_query(F.data == "nav:start")
async def back_to_start(cb: CallbackQuery):
    ensure_user(cb.from_user.id)
    await send_home(chat_id=cb.message.chat.id, user_id=cb.from_user.id)
    await cb.answer()

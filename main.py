# main.py — FastAPI + aiogram v3.x (Render webhook)
import os
import re
import csv
from io import StringIO
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone

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

def ensure_user(user_id: int) -> None:
    USER_PREFS.setdefault(user_id, {"active_db": "default"})
    USER_STATE.setdefault(user_id, {})

def get_active_db(user_id: int) -> str:
    return USER_PREFS.get(user_id, {}).get("active_db", "default")

def set_active_db(user_id: int, dbname: str) -> None:
    USER_PREFS.setdefault(user_id, {})["active_db"] = dbname

# ----------------- Utils parsing & stats -----------------
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

# ----------------- Accueil (sans emojis ni statistiques) -----------------
async def send_home(chat_id: int, user_id: int):
    active_db = get_active_db(user_id)

    text = (
        "Bienvenue sur FICHES CLIENTS\n\n"
        f"Base active : `{active_db}`\n\n"
        "Choisissez une action ci-dessous.\n"
        "Tapez /start à tout moment pour revenir à l'accueil."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗄️ Gérer les bases", callback_data="home:db")],
        [InlineKeyboardButton(text="🔎 Rechercher une fiche", callback_data="home:search")],
        [InlineKeyboardButton(text="📵 Appels manqués (0)", callback_data="home:missed")],
        [InlineKeyboardButton(text="🗂️ Dossiers en cours (0)", callback_data="home:cases")],
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

# ----------------- Rechercher une fiche (bouton) -----------------
@router.callback_query(F.data == "home:search")
async def start_search(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    USER_STATE[user_id]["awaiting_search_number"] = True
    text = (
        "Recherche par numéro\n\n"
        "Envoie un numéro au format `06123456789`.\n"
        "Je cherche dans la base active et j’affiche la fiche si elle existe."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Retour", callback_data="nav:start")]
    ])
    try:
        await cb.message.edit_caption(caption=text, parse_mode="Markdown", reply_markup=kb)
    except Exception:
        try:
            await cb.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            await cb.message.answer(text, reply_markup=kb, parse_mode="Markdown")
    await cb.answer()

# ----------------- /num : recherche par numéro (commande) -----------------
def render_record(rec: Dict) -> str:
    last = rec.get("last_name") or ""
    first = rec.get("first_name") or ""
    name = (last + (" - " + first if first else "")) if (last or first) else (rec.get("full_name_raw") or "—")
    mobile = rec.get("mobile") or "—"
    voip = rec.get("voip") or "—"
    email = rec.get("email") or "—"
    ville = rec.get("ville") or "—"
    cp = rec.get("cp") or "—"
    adr = rec.get("adresse") or "—"
    iban = rec.get("iban") or "—"
    bic = rec.get("bic") or "—"
    return (
        f"Fiche trouvée\n"
        f"- Nom : {name}\n"
        f"- Mobile : {mobile}\n"
        f"- VoIP : {voip}\n"
        f"- Email : {email}\n"
        f"- Adresse : {adr}\n"
        f"- Ville : {ville} ({cp})\n"
        f"- IBAN : {iban}\n"
        f"- BIC : {bic}"
    )

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
    matches = [r for r in records if r.get("mobile") == num or r.get("voip") == num]

    if not matches:
        await message.answer(f"Aucune fiche trouvée pour le numéro {num}.")
        return

    if len(matches) == 1:
        await message.answer(render_record(matches[0]))
        return

    lines = [f"{len(matches)} fiches trouvées pour {num} :", ""]
    for i, r in enumerate(matches[:10], start=1):
        last = r.get("last_name") or ""
        first = r.get("first_name") or ""
        name = (last + (" - " + first if first else "")) if (last or first) else (r.get("full_name_raw") or "—")
        ville = r.get("ville") or "—"
        cp = r.get("cp") or "—"
        lines.append(f"{i}. {name} — {ville} ({cp})")
    if len(matches) > 10:
        lines.append("…")
    await message.answer("\n".join(lines))

@router.message(Command("num"))
async def search_by_number_cmd(message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Utilisation : /num 06123456789")
        return
    await find_and_reply_number(message, parts[1])

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

async def edit_home_like(cb: CallbackQuery, text: str, kb: InlineKeyboardMarkup, parse_mode: Optional[str] = "Markdown"):
    try:
        await cb.message.edit_caption(caption=text, parse_mode=parse_mode, reply_markup=kb)
        return
    except Exception:
        pass
    try:
        await cb.message.edit_text(text, parse_mode=parse_mode, reply_markup=kb)
        return
    except Exception:
        pass
    await cb.message.answer(text, reply_markup=kb, parse_mode=parse_mode)

@router.callback_query(F.data == "home:db")
async def open_db_list(cb: CallbackQuery):
    user_id = cb.from_user.id
    ensure_user(user_id)
    text = render_db_list_text_only()
    kb = db_list_keyboard(user_id)
    await edit_home_like(cb, text, kb)
    await cb.answer()

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
        await cb.answer("Base introuvable.", show_alert=True)
        return
    set_active_db(user_id, name)
    text = f"Base sélectionnée : `{name}`\n\nChoisissez une action."
    kb = base_menu_keyboard(name)
    await edit_home_like(cb, text, kb)
    await cb.answer()

# ----------------- Saisies texte : nom de base ou numéro recherché -----------------
@router.message(F.text)
async def capture_search_or_name(message: Message):
    user_id = message.from_user.id
    ensure_user(user_id)

    # 1) Création base : on attend un nom
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

        # Demander directement le fichier d'import
        USER_STATE[user_id]["awaiting_import_for_base"] = raw
        text = (
            f"Base créée : `{raw}`\n\n"
            "Envoie maintenant le fichier d’import :\n"
            "- .txt (format fourni), .csv, ou .jsonl\n"
            "Les gros fichiers peuvent être découpés.\n\n"
            "Quand l’import sera terminé, j’afficherai le menu de la base."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Retour", callback_data="home:db")]
        ])
        await message.answer(text, reply_markup=kb, parse_mode="Markdown")
        return

    # 2) Recherche par numéro (déclenchée par le bouton)
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
    await edit_home_like(cb, text, kb)
    await cb.answer()

# ----------------- Statistiques (départements uniquement) -----------------
def sorted_dept_counts(counts: Dict[str,int]) -> List[Tuple[str,int]]:
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))

@router.callback_query(F.data.startswith("db:stats:"))
async def db_stats(cb: CallbackQuery):
    name = cb.data.split(":", 2)[2]
    meta = BASES.get(name)
    if not meta:
        await cb.answer("Base introuvable.", show_alert=True)
        return

    total = meta["records"]
    depts = sorted_dept_counts(meta.get("dept_counts", {}))
    if depts:
        lines = [f"Statistiques — `{name}`", "", f"Total fiches : {total}", ""]
        for code, n in depts:
            lines.append(f"- {code} : {n} fiche(s)")
        text = "\n".join(lines)
    else:
        text = f"Statistiques — `{name}`\n\nTotal fiches : {total}\n\nAucun département détecté."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Retour", callback_data=f"db:open:{name}")]
    ])
    await edit_home_like(cb, text, kb)
    await cb.answer()

# ----------------- Import -----------------
@router.callback_query(F.data.startswith("db:import:"))
async def db_import_start(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await cb.answer("Base introuvable.", show_alert=True)
        return
    ensure_user(user_id)
    set_active_db(user_id, name)
    USER_STATE[user_id]["awaiting_import_for_base"] = name

    text = (
        f"Import dans la base « `{name}` ».\n\n"
        "Envoie un fichier .txt (format fourni), .csv ou .jsonl.\n"
        "Les fichiers volumineux peuvent être découpés."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Retour", callback_data=f"db:open:{name}")]
    ])
    await edit_home_like(cb, text, kb)
    await cb.answer()

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

    text = f"Base sélectionnée : `{target}`\n\nChoisissez une action."
    kb = base_menu_keyboard(target)
    await message.answer(text, reply_markup=kb, parse_mode="Markdown")

# ----------------- Export CSV -----------------
@router.callback_query(F.data.startswith("db:export:"))
async def db_export(cb: CallbackQuery):
    name = cb.data.split(":", 2)[2]
    meta = BASES.get(name)
    if not meta:
        await cb.answer("Base introuvable.", show_alert=True)
        return

    headers = ["last_name", "first_name", "full_name_raw", "email", "mobile", "voip",
               "ville", "cp", "dept", "adresse", "iban", "bic", "dob", "statut"]
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for rec in meta.get("records_list", []):
        writer.writerow(rec)
    csv_data = buf.getvalue().encode("utf-8")

    tmp_path = f"/tmp/export_{name}_{int(datetime.now().timestamp())}.csv"
    with open(tmp_path, "wb") as f:
        f.write(csv_data)

    await cb.message.answer_document(
        document=FSInputFile(tmp_path, filename=f"{name}.csv"),
        caption=f"Export CSV — {name} ({len(meta.get('records_list', []))} fiches)."
    )
    await cb.answer()

# ----------------- Supprimer (depuis le menu de la base) -----------------
@router.callback_query(F.data.startswith("db:drop:"))
async def db_drop(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await cb.answer("Base introuvable.", show_alert=True)
        return
    if get_active_db(user_id) != name:
        await cb.answer("Clique d'abord sur la base pour la sélectionner, puis supprime.", show_alert=True)
        return

    text = f"Confirmer la suppression de la base « {name} » ? Action définitive."
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Supprimer définitivement", callback_data=f"db:dropconfirm:{name}")],
        [InlineKeyboardButton(text="Retour", callback_data=f"db:open:{name}")]
    ])
    try:
        await cb.message.edit_caption(caption=text, reply_markup=kb)
    except Exception:
        try:
            await cb.message.edit_text(text, reply_markup=kb)
        except Exception:
            await cb.message.answer(text, reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("db:dropconfirm:"))
async def db_drop_confirm(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.data.split(":", 2)[2]
    if name not in BASES:
        await cb.answer("Base introuvable.", show_alert=True)
        return
    if get_active_db(user_id) != name:
        await cb.answer("Clique d'abord sur la base pour la sélectionner, puis supprime.", show_alert=True)
        return
    if len(BASES) == 1:
        await cb.answer("Impossible: il doit rester au moins une base.", show_alert=True)
        return

    del BASES[name]
    set_active_db(user_id, "default" if "default" in BASES else next(iter(BASES.keys())))
    await cb.answer("Base supprimée.")

    text = render_db_list_text_only()
    kb = db_list_keyboard(user_id)
    try:
        await cb.message.edit_caption(caption=text, reply_markup=kb)
    except Exception:
        try:
            await cb.message.edit_text(text, reply_markup=kb)
        except Exception:
            await cb.message.answer(text, reply_markup=kb)

# ----------------- Retour accueil -----------------
@router.callback_query(F.data == "nav:start")
async def back_to_start(cb: CallbackQuery):
    ensure_user(cb.from_user.id)
    USER_STATE[cb.from_user.id]["awaiting_search_number"] = False
    await send_home(chat_id=cb.message.chat.id, user_id=cb.from_user.id)
    await cb.answer()

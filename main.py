# main.py — FastAPI + aiogram v3.x (compatible Render webhook)
import os
from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Update

# ----------------- Configuration -----------------
TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    raise RuntimeError("Missing TELEGRAM_TOKEN environment variable")

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()
app = FastAPI()

# ----------------- Accueil (/start, /home) -----------------
@router.message(CommandStart())
@router.message(Command("home"))
async def accueil(message: types.Message):
    # Valeurs temporaires — à relier plus tard à ta base de données
    active_db = "default"
    nb_contactes = 0                 # Clients traités aujourd'hui
    nb_appels_manques = 0            # Appels manqués à traiter
    nb_dossiers_en_cours = 0         # Dossiers en cours (suivis ouverts)

    # Message d'accueil amélioré
    text = (
        "Bienvenue sur FICHES CLIENTS.\n\n"
        f"📁 Base active : {active_db}\n\n"
        "Statistiques du jour :\n"
        f"- Clients traités : {nb_contactes}\n"
        f"- Appels manqués à gérer : {nb_appels_manques}\n"
        f"- Dossiers en cours : {nb_dossiers_en_cours}\n\n"
        "Sélectionnez une action ci-dessous pour continuer :"
    )

    # Boutons d’action
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Gérer les bases", callback_data="home:db")],
        [InlineKeyboardButton(text="Rechercher une fiche", callback_data="home:search")],
        [InlineKeyboardButton(text=f"Appels manqués ({nb_appels_manques})", callback_data="home:missed")],
        [InlineKeyboardButton(text=f"Dossiers en cours ({nb_dossiers_en_cours})", callback_data="home:cases")],
    ])

    # Image d'accueil
    image_url = "https://i.postimg.cc/0jNN08J5/IMG-0294.jpg"

    # Envoi de l’image + texte + boutons
    await message.answer_photo(
        photo=image_url,
        caption=text,
        reply_markup=kb
    )

# (tu ajouteras ensuite des handlers pour les callback_data ci-dessus)

# Enregistre le router dans le dispatcher
dp.include_router(router)

# ----------------- FastAPI: health -----------------
@app.get("/")
async def health():
    return {"status": "ok"}

# ----------------- FastAPI: endpoint Telegram webhook -----------------
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

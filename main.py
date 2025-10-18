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
    # placeholders (on branchera sur la DB ensuite)
    active_db = "default"
    nb_contactes = 0
    nb_rappels = 0

    text = (
        "Bienvenue.\n"
        f"Base active : {active_db}\n\n"
        f"Clients traités aujourd'hui : {nb_contactes}\n"
        f"Clients à rappeler : {nb_rappels}\n\n"
        "Choisissez une action :"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Gérer les bases", callback_data="home:db")],
        [InlineKeyboardButton(text="Rechercher une fiche", callback_data="home:search")],
        [InlineKeyboardButton(text=f"Appels manqués ({nb_rappels})", callback_data="home:missed")],
        [InlineKeyboardButton(text="Notes", callback_data="home:notes")],
    ])
    await message.answer(text, reply_markup=kb)

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

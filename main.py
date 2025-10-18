# ---------- ACCUEIL ----------
@dp.message_handler(commands=["start", "home"])
async def accueil(message: types.Message):
    user_id = message.from_user.id

    # Exemple de récupération du rôle et de la base active
    role = get_user_role(user_id)  # fonction à créer plus tard
    active_db = get_active_db(user_id) or "default"

    # Exemple de récupération des compteurs (à implémenter ensuite)
    nb_contactes = get_count_contactes_today(user_id)  # clients traités aujourd’hui
    nb_rappels = get_count_a_rappeler(user_id)         # clients à rappeler

    # Texte d’accueil
    text = (
        f"Bienvenue.\n"
        f"Base active : {active_db}\n\n"
        f"📞 Clients traités aujourd’hui : {nb_contactes}\n"
        f"⏰ Clients à rappeler : {nb_rappels}\n\n"
        f"Choisissez une action :"
    )

    # Boutons principaux
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("Gérer les bases", callback_data="home:db"),
        InlineKeyboardButton("Rechercher une fiche", callback_data="home:search"),
        InlineKeyboardButton(f"Appels manqués ({nb_rappels})", callback_data="home:missed"),
        InlineKeyboardButton("Notes", callback_data="home:notes")
    )

    # Envoi ou édition du message d’accueil
    try:
        await message.edit_text(text, reply_markup=keyboard)
    except:
        await message.answer(text, reply_markup=keyboard)

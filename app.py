import psycopg2
import json
import requests
import os
import logging
from dotenv import load_dotenv

# ------------------ Setup ------------------ #
load_dotenv()

logging.basicConfig(
    filename="log_file.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

CHARACTER_URL = "https://hp-api.onrender.com/api/characters"
SPELL_URL = "https://hp-api.onrender.com/api/spells"

DB_CONFIG = {
    "host":os.getenv("YOUR_HOST_NAME"),
    "user":os.getenv("YOUR_USER_NAME"),
    "password":os.getenv("YOUR_PASSWORD"),
    "database":os.getenv("YOUR_DATABASE")
}

# ------------------ Fetch API ------------------ #
try:
    character_response = requests.get(CHARACTER_URL)
    character_response.raise_for_status()
    character_data = character_response.json()

    spell_response = requests.get(SPELL_URL)
    spell_response.raise_for_status()
    spell_data = spell_response.json()

    with open("characters.json", "w") as f:
        json.dump(character_data, f, indent=4)

    with open("spells.json", "w") as f:
        json.dump(spell_data, f, indent=4)

    logging.info("Data fetched successfully")

except Exception as e:
    logging.error(f"Error fetching data: {e}")
    exit()

# ------------------ DB Connection ------------------ #
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    logging.info("Connected to database")

except Exception as e:
    logging.error(f"DB Connection failed: {e}")
    exit()

# ------------------ Create Tables ------------------ #
cursor.execute("""
CREATE TABLE IF NOT EXISTS characters (
    id UUID PRIMARY KEY,
    name TEXT,
    gender TEXT,
    house TEXT,
    year_of_birth INT,
    wizard BOOLEAN,
    ancestry TEXT,
    eye_colour TEXT,
    hair_colour TEXT,
    wand_wood TEXT,
    wand_core TEXT,
    wand_length FLOAT,
    patronus TEXT,
    actor TEXT,
    alive BOOLEAN
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS spells (
    name TEXT PRIMARY KEY,
    description TEXT
);
""")

conn.commit()

# ------------------ Insert Characters ------------------ #
char_insert_query = """
INSERT INTO characters VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
"""

char_count = 0

for char in character_data:
    try:
        wand = char.get("wand", {})

        cursor.execute(char_insert_query, (
            char.get("id"),
            char.get("name"),
            char.get("gender"),
            char.get("house"),
            char.get("yearOfBirth"),
            char.get("wizard"),
            char.get("ancestry"),
            char.get("eyeColour"),
            char.get("hairColour"),
            wand.get("wood"),
            wand.get("core"),
            wand.get("length"),
            char.get("patronus"),
            char.get("actor"),
            char.get("alive")
        ))

        char_count += 1

    except Exception as e:
        logging.warning(f"Skipping character: {e}")

# ------------------ Insert Spells ------------------ #
spell_insert_query = """
INSERT INTO spells (name, description)
VALUES (%s, %s)
ON CONFLICT (name) DO NOTHING;
"""

spell_count = 0

for spell in spell_data:
    try:
        cursor.execute(spell_insert_query, (
            spell.get("name"),
            spell.get("description")
        ))
        spell_count += 1

    except Exception as e:
        logging.warning(f"Skipping spell: {e}")

conn.commit()

# ------------------ Cleanup ------------------ #
cursor.close()
conn.close()

logging.info(f"Inserted {char_count} characters")
logging.info(f"Inserted {spell_count} spells")





















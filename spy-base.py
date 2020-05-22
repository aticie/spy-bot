import os
import time
import sqlite3
import pickle
import asyncio
from random import SystemRandom

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import aiohttp
from discord.ext import tasks, commands

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

creds = None
# The file token.pickle stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()


@tasks.loop(minutes=5)
async def post_results():
    await bot.wait_until_ready()
    print("Starting post results")

    conn = sqlite3.connect("spy.db")
    c = conn.cursor()

    with open("players.txt", "r") as f:
        opponent_players = f.read().splitlines()

    with open("beatmaps.txt", "r") as f:
        beatmaps = f.read().splitlines()

    all_scores = []
    for bmap in beatmaps:
        bmap_scores = []
        bmap_id = int(bmap)
        for username in opponent_players:
            scores = c.execute("SELECT * FROM scores WHERE username=? AND bmap_id=?", [username, bmap_id]).fetchall()
            if len(scores) == 0:
                avg_score = None
            else:
                avg_score = f"{int(sum(sc for _, _, _, sc, _ in scores) / len(scores))}"
            bmap_scores.append(avg_score)
        all_scores.append(bmap_scores)

    data = [
        {
            'range': f"OpponentScores-QF!F2:M21",
            'values': all_scores
        },
        # Additional ranges to update ...
    ]
    body = {
        'valueInputOption': 2,
        'data': data
    }
    now = time.strftime("%H:%M:%S")
    print(f"{now} - Sending the following to spreadsheet!")
    for sc in all_scores:
        print(sc)
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body=body).execute()

    # Dump to another sheet
    dumped_scores = c.execute("SELECT * FROM scores").fetchall()
    dumped_scores_reshape = [[uname, bmap_id, sc, date] for _, uname, bmap_id, sc, date in dumped_scores]
    data = [
        {
            'range': f"OpponentScores-QF-Dump!A2:D{len(dumped_scores)+2}",
            'values': dumped_scores_reshape
        },
        # Additional ranges to update ...
    ]
    body = {
        'valueInputOption': 2,
        'data': data
    }
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body=body).execute()


@tasks.loop(minutes=15)
async def spy_user():
    await bot.wait_until_ready()

    conn = sqlite3.connect("spy.db")
    c = conn.cursor()

    with open("players.txt", "r") as f:
        opponent_players = f.read().splitlines()

    with open("beatmaps.txt", "r") as f:
        beatmaps = f.read().splitlines()

    for username in opponent_players:

        scores = await request_scores(username)
        sleep_for = SystemRandom().random() * 3 + 2  # Sleep between 2-5 seconds
        await asyncio.sleep(sleep_for)

        now = time.strftime("%H:%M:%S")
        print(f"{now} - Checking scores for {username}, played {len(scores)} scores recently.")
        for score in scores:
            mods = int(score["enabled_mods"])
            sv2_enabled = mods & 536870912 == 536870912

            if score["beatmap_id"] in beatmaps:
                score_mode_text = "sv2" if sv2_enabled else "sv1"
                fail_text = "(Failed)" if score["rank"] == "F" else ""
                print(
                    f"{username} played {score['beatmap_id']} -"
                    f" It was {score_mode_text} and he made {score['score']}.{fail_text}")

            if score["beatmap_id"] in beatmaps and not score["rank"] == "F" and sv2_enabled:
                now = time.strftime("%H:%M:%S")
                print(f"{now} - Adding score of {username} to DB. - Beatmap {score['beatmap_id']}")
                add_to_db_if_not_exists(c, score, username)

        conn.commit()


def add_to_db_if_not_exists(cursor, score, username):
    bmap_id = int(score["beatmap_id"])
    date = score["date"]
    player_score = int(score["score"])
    user_id = int(score["user_id"])
    db_score = cursor.execute("SELECT * FROM scores WHERE user_id=? AND bmap_id=? AND score=? AND date=?",
                              [user_id, bmap_id, player_score, date]).fetchone()
    if db_score is None:
        cursor.execute("INSERT INTO scores VALUES (?,?,?,?,?)", [user_id, username, bmap_id, player_score, date])

    return


async def request_scores(username):
    api_url = f"https://osu.ppy.sh/api/get_user_recent"
    params = {"u": username,
              "k": os.environ["OSU_API_KEY"],
              "limit": 50,
              "m": 0
              }
    async with aiohttp.ClientSession() as s:
        async with s.get(api_url, params=params) as r:
            response = await r.json()

    return response


bot = commands.Bot(command_prefix="?", case_insensitive=True, description="Just an ordinary bot, nothing to see here")


@bot.event
async def on_ready():
    spy_user.start()
    post_results.start()


bot.run(os.environ["DISCORD_TOKEN"], bot=True, reconnect=True)

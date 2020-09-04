import os
import time
import json
import itertools
import sqlite3
import pickle
import asyncio
from random import SystemRandom

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import aiohttp
from discord.ext import tasks, commands
import discord

bot = commands.Bot(command_prefix="?", case_insensitive=True, description="Just an ordinary bot, nothing to see here")

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

with open('secrets.json', 'r') as f:
    secrets = json.load(f)

SPREADSHEET_ID = secrets['spreadsheet_id']
DISCORD_TOKEN = secrets['discord_token']
OSU_API_KEYS = secrets['api_keys']

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


async def post_results():
    await bot.wait_until_ready()
    print("Starting post results")

    conn = sqlite3.connect("spy.db")
    c = conn.cursor()

    with open("players.txt", "r") as f:
        players = f.read().splitlines()

    with open("beatmaps.txt", "r") as f:
        beatmaps = f.read().splitlines()

    all_scores = []
    for bmap in beatmaps:
        bmap_scores = []
        bmap_id = int(bmap)
        none_fill = [None for _ in range(35)]
        bmap_scores.append(none_fill)
        bmap_scores.append([])
        for username in players:
            scores = c.execute("SELECT * FROM scores WHERE username=? AND bmap_id=?", [username, bmap_id]).fetchall()

            scores_array = [sc for _, _, _, sc, _ in scores]
            scores_array.sort(reverse=True)

            top4_scores = []
            for i in range(5):
                try:
                    top4_scores.append(scores_array[i])
                except IndexError:
                    top4_scores.append(None)

            bmap_scores[-1].extend(top4_scores)

        bmap_scores.append(none_fill)
        all_scores.extend(bmap_scores)

    post_to_sheet("Qualifier", all_scores, False)
    # Dump to another sheet
    dumped_scores = c.execute("SELECT * FROM scores").fetchall()
    post_to_sheet("ScoresDump", dumped_scores, True)
    return


def post_to_sheet(sheet_name, values, dump_or_not):
    if not dump_or_not:
        data = [
            {
                'range': f"{sheet_name}!C3:AK37",
                'values': values
            },
            # Additional ranges to update ...
        ]
        body = {
            'valueInputOption': 2,
            'data': data
        }
        now = time.strftime("%H:%M:%S")
        print(f"{now} - Sending scores to spreadsheet!")
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body=body).execute()
    else:
        values_reshape = [[uname, bmap_id, sc, date] for _, uname, bmap_id, sc, date in values]
        data = [
            {
                'range': f"{sheet_name}!A2:D{len(values) + 2}",
                'values': values_reshape
            },
            # Additional ranges to update ...
        ]
        body = {
            'valueInputOption': 2,
            'data': data
        }
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body=body).execute()
    return


async def spy_user():
    await bot.wait_until_ready()

    conn = sqlite3.connect("spy.db")
    c = conn.cursor()

    with open("players.txt", "r") as f:
        players = f.read().splitlines()

    with open("beatmaps.txt", "r") as f:
        beatmaps = f.read().splitlines()

    user_cycle = itertools.cycle(players)
    keys_cycle = itertools.cycle(OSU_API_KEYS)
    while True:
        username = next(user_cycle)
        selected_api_key = next(keys_cycle)
        scores = await request_scores(username, selected_api_key)
        sleep_for = SystemRandom().random() * 3 + 2  # Sleep between 2-5 seconds
        await asyncio.sleep(sleep_for)

        now = time.strftime("%H:%M:%S")
        print(f"{now} - Checking scores for {username}, played {len(scores)} scores recently.")
        await add_scores_to_db(scores, beatmaps, username, c, selected_api_key)

        conn.commit()

        await post_results()


async def add_scores_to_db(scores, beatmaps, username, cursor, api_key):
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
            await add_to_db_if_not_exists(cursor, score, username, api_key)


async def add_to_db_if_not_exists(cursor, score, username, api_key):
    bmap_id = int(score["beatmap_id"])
    date = score["date"]
    player_score = int(score["score"])
    user_id = int(score["user_id"])
    db_score = cursor.execute("SELECT * FROM scores WHERE user_id=? AND bmap_id=? AND score=? AND date=?",
                              [user_id, bmap_id, player_score, date]).fetchone()
    if db_score is None:
        cursor.execute("INSERT INTO scores VALUES (?,?,?,?,?)", [user_id, username, bmap_id, player_score, date])
        score_embed = await make_score_embed(*[user_id, username, bmap_id, player_score, date, api_key])
        score_channel = bot.get_channel(749956966373261362)  # 749956966373261362, test: 676411865592758272
        await score_channel.send(embed=score_embed)

    return


async def make_score_embed(user_id, username, bmap_id, player_score, date, api_key):
    api_url = f"https://osu.ppy.sh/api/get_beatmaps"
    params = {"k": api_key,
              "b": bmap_id
              }
    async with aiohttp.ClientSession() as s:
        async with s.get(api_url, params=params) as r:
            bmap_info = await r.json()

    bmap_info = bmap_info[0]
    beatmapset_id = bmap_info['beatmapset_id']
    bmap_title = bmap_info['title']
    thumb_url = f'https://b.ppy.sh/thumb/{beatmapset_id}l.jpg'
    title_text = f'{username} just scored: **{player_score}**!'
    embed = discord.Embed(title=title_text)
    embed.set_author(name=f"New score from {username} on {bmap_title}!", url=f"https://osu.ppy.sh/users/{user_id}",
                     icon_url=f"http://s.ppy.sh/a/{user_id}")
    embed.set_thumbnail(url=thumb_url)
    embed.set_footer(text=f'Score date: {date}')
    return embed


async def request_scores(username, api_key):
    api_url = f"https://osu.ppy.sh/api/get_user_recent"
    params = {"u": username,
              "k": api_key,
              "limit": 100,
              "m": 0
              }
    async with aiohttp.ClientSession() as s:
        async with s.get(api_url, params=params) as r:
            response = await r.json()

    return response


@bot.event
async def on_ready():
    await spy_user()


bot.run(DISCORD_TOKEN, bot=True, reconnect=True)

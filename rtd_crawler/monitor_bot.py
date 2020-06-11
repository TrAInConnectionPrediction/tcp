# bot.py
import os

import discord
from config import discord_bot_token
from DatabaseOfDoom import DatabaseOfDoom
import datetime
from time import sleep

client = discord.Client()

db = DatabaseOfDoom()

@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')
    channel = client.get_channel(720671295129518232)

    hour = datetime.datetime.now().time().hour - 1
    await channel.send('Data gatherer monitor now active')
    while True:
        if hour == datetime.datetime.now().time().hour:
            sleep(20)
        else:
            hour = datetime.datetime.now().time().hour
            date_to_check = datetime.datetime.combine(datetime.date.today(), datetime.time(hour, 0)) - datetime.timedelta(hours=1)
            new_row_conts = db.count_entrys_at_date(date_to_check)
            if new_row_conts < 7000:
                await channel.send('@everyone The gatherer is not working, as ' + str(new_row_conts) + ' new entrys where added to database at ' + str(date_to_check))

@client.event
async def on_message(message):
    if message.author == client.user:
        return
    print(message.channel.id)

if __name__ == "__main__":
    client.run(discord_bot_token)

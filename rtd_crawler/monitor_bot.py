import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if os.path.isfile("/mnt/config/config.py"):
    sys.path.append("/mnt/config/")
import discord
from discord.ext import tasks, commands
from config import discord_bot_token
import datetime
import requests
from database import Change, Plan

client = discord.Client()

old_change_count = 0


@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')
    channel = client.get_channel(720671295129518232)
    await channel.send('Data gatherer monitor now active')


@client.event
async def monitor_hour(old_change_count):
    channel = client.get_channel(720671295129518232)
    hour = datetime.datetime.now().time().hour - 1

    hour = datetime.datetime.now().time().hour
    date_to_check = datetime.datetime.combine(datetime.date.today(),
                                              datetime.time(hour, 0)) - datetime.timedelta(hours=1)

    # Plan (hourly)
    try:
        with Plan() as plan:
            plan_row_count = plan.count_entries_at_date(date_to_check)
        if plan_row_count < 7000:
            message = '@everyone The plan gatherer is not working, as {} new entries where added to database at {}'\
                    .format(str(plan_row_count), str(date_to_check))
            await channel.send(message)
        print('checked plan ' + str(date_to_check) + ': ' + str(plan_row_count) + ' rows were added')
    except Exception as ex:
        message = '@everyone Error reading Database:\n{}'.format(str(ex))
        await channel.send(message)
        print('checked ' + str(date_to_check) + ': ???? rows were added')

    # Recent changed (crawled every two minutes but only checked once a day)
    if hour == 6:
        try:
            with Change() as changes:
                new_change_count = changes.count_entries()
            count_delta = new_change_count - old_change_count
            if count_delta < 50000:
                message = '''@everyone The recent change gatherer is not working, as {} 
                        new entries where added to database at {}'''\
                        .format(str(count_delta), str(date_to_check))
                await channel.send(message)
            old_change_count = new_change_count
            print('checked changes ' + str(date_to_check) + ': ' + str(count_delta) + ' rows were added')
        except Exception as ex:
            message = '@everyone Error reading Database:\n{}'.format(str(ex))
            await channel.send(message)
            print('checked ' + str(date_to_check) + ': ???? rows were added')

    return old_change_count

@client.event
async def monitor_website():
    channel = client.get_channel(720671295129518232)
    try:
        print('testing https://trainconnectionprediction.de...')
        page = requests.get('https://trainconnectionprediction.de')
        if page.ok:
            print('ok')
        else:
            message = str(datetime.datetime.now()) \
                + ': @everyone Somthing not working on the website:\n{}'.format(str(page))
            print(message)
            # await channel.send(message)
    except Exception as ex:
        message = str(datetime.datetime.now()) + ': @everyone Error on the website:\n{}'.format(str(ex))
        print(message)
        # await channel.send(message)

    try:
        print('testing https://trainconnectionprediction.de/api/trip from Tübingen Hbf to Köln Hbf...')
        search = {
            'start': 'Tübingen Hbf',
            'destination': 'Köln Hbf',
            'date': (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime('%d.%m.%Y %H:%M')
        }
        trip = requests.post('https://trainconnectionprediction.de/api/trip', json=search)
        if trip.ok:
                print('ok')
        else:
            message = str(datetime.datetime.now()) \
                + ': @everyone Somthing not working on the website:\n{}'.format(str(trip))
            print(message)
            await channel.send(message)
    except Exception as ex:
        message = str(datetime.datetime.now()) + ': @everyone Somthing not working on the website:\n{}'.format(str(ex))
        print(message)
        await channel.send(message)


class Monitor(commands.Cog):
    def __init__(self):
        self.monitor.start()
        self.old_change_count = 0

    def monitor_unload(self):
        self.monitor.cancel()

    @tasks.loop(hours=1)
    async def monitor(self):
        await client.wait_until_ready()
        self.old_change_count = await monitor_hour(self.old_change_count)
        await monitor_website()


if __name__ == "__main__":
    import helpers.fancy_print_tcp

    m = Monitor()
    client.run(discord_bot_token)

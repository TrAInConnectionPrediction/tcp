import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if os.path.isfile("/mnt/config/config.py"):
    sys.path.append("/mnt/config/")
import discord
from discord.ext import tasks, commands
from config import discord_bot_token
import datetime
import requests
from database import Change, PlanById, sessionfactory
from typing import Callable, Coroutine, Union
import traceback
import functools
import asyncio


def to_thread(func: Callable) -> Coroutine:
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        wrapped = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, wrapped)
    return wrapper


engine, Session = sessionfactory()
client = discord.Client()
old_change_count = 0
old_plan_count = 0


@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')
    channel = client.get_channel(720671295129518232)
    await channel.send('Data gatherer monitor now active')


@to_thread
def monitor_plan() -> Union[str, None]:
    global old_plan_count

    date_to_check = datetime.datetime.combine(
        datetime.date.today(),
        (datetime.datetime.now() + datetime.timedelta(hours=3)).time()
    )

    # Plan (hourly)
    message = None
    try:
        with Session() as session:
            new_plan_count = PlanById.count_entries(session)
        plan_count_delta = new_plan_count - old_plan_count
        old_plan_count = new_plan_count
        if plan_count_delta < 500:
            message = '@everyone The plan gatherer is not working, as {} new entries where added to database at {}'\
                    .format(str(plan_count_delta), str(date_to_check))
        print('checked plan ' + str(date_to_check) + ': ' + str(plan_count_delta) + ' rows were added')
    except FileExistsError as e:
        message = '@everyone Error reading Database:\n{}' \
            .format(''.join(traceback.format_exception(e, e, e.__traceback__)))
        print(message)
        print('checked ' + str(date_to_check) + ': ???? rows were added')
    
    return message


@to_thread
def monitor_change() -> Union[str, None]:
    global old_change_count

    date_to_check = datetime.datetime.combine(
        datetime.date.today(),
        (datetime.datetime.now() + datetime.timedelta(hours=3)).time()
    )

    # Recent changed (crawled every two minutes but only checked once an hour)
    message = None
    try:
        with Session() as session:
            new_change_count = Change.count_entries(session)
        count_delta = new_change_count - old_change_count
        old_change_count = new_change_count
        if count_delta < 1000:
            message = '''@everyone The recent change gatherer is not working, as {} 
                    new entries where added to database at {}'''\
                    .format(str(count_delta), str(date_to_check))
        old_change_count = new_change_count
        print('checked changes ' + str(date_to_check) + ': ' + str(count_delta) + ' rows were added')
    except Exception as e:
        message = '@everyone Error reading Database:\n{}' \
            .format(''.join(traceback.format_exception(None, e, e.__traceback__)))
        print(message)
        print('checked ' + str(date_to_check) + ': ???? rows were added')

    return message


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
            await channel.send(message)
    except Exception as ex:
        message = str(datetime.datetime.now()) + ': @everyone Error on the website:\n{}'.format(str(ex))
        print(message)
        await channel.send(message)

    try:
        print('testing https://trainconnectionprediction.de/api/trip from Tübingen Hbf to Köln Hbf...')
        search = {
            'start': 'Tübingen Hbf',
            'destination': 'Köln Hbf',
            'date': (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime('%d.%m.%Y %H:%M'),
            'search_for_departure': True
        }
        trip = requests.post('https://trainconnectionprediction.de/api/trip', json=search)
        if trip.ok:
                print('ok')
        else:
            message = str(datetime.datetime.now()) \
                + ': @everyone Somthing not working on the website:\n{}'.format(str(trip))
            print(message)
            await channel.send(message)
    except Exception as e:
        message = str(datetime.datetime.now()) + ': @everyone Somthing not working on the website:\n{}' \
            .format(''.join(traceback.format_exception(None, e, e.__traceback__)))
        await channel.send(message)
        print(message)


class Monitor(commands.Cog):
    def __init__(self):
        self.monitor.start()
        self.old_change_count = 0

    def monitor_unload(self):
        self.monitor.cancel()

    @tasks.loop(hours=1)
    async def monitor(self):
        await client.wait_until_ready()
        channel = client.get_channel(720671295129518232)

        message = await monitor_plan()
        if message is not None:
            await channel.send(message)

        message = await monitor_change()
        if message is not None:
            await channel.send(message)

        await monitor_website()


if __name__ == "__main__":
    import helpers.fancy_print_tcp

    m = Monitor()
    client.run(discord_bot_token)

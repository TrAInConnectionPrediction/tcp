import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import discord
from discord.ext import tasks, commands
from config import discord_bot_token
from rtd_crawler.DatabaseOfDoom import DatabaseOfDoom
import datetime
from database.change import ChangeManager
from database.plan import PlanManager
from time import sleep

client = discord.Client()

db = DatabaseOfDoom()
changes = ChangeManager()
plan = PlanManager()


@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')
    channel = client.get_channel(720671295129518232)
    await channel.send('Data gatherer monitor now active')


@client.event
async def monitor_hour():
    channel = client.get_channel(720671295129518232)
    # channel = discord.Object(id=720671295129518232)
    hour = datetime.datetime.now().time().hour - 1
    old_change_count = 0

    hour = datetime.datetime.now().time().hour
    date_to_check = datetime.datetime.combine(datetime.date.today(),
                                              datetime.time(hour, 0)) - datetime.timedelta(hours=1)
    new_row_cont = db.count_entries_at_date(date_to_check)
    if new_row_cont < 7000:
        message = '@everyone The gatherer is not working, as {} new entries where added to database at {}'\
                  .format(str(new_row_cont), str(date_to_check))
        await channel.send(message)
    print('checked ' + str(date_to_check) + ': ' + str(new_row_cont) + ' rows were added')

    plan_row_count = plan.count_entries_at_date(date_to_check)
    if plan_row_count < 7000:
        message = '@everyone The plan gatherer is not working, as {} new entries where added to database at {}'\
                  .format(str(plan_row_count), str(date_to_check))
        await channel.send(message)
    print('checked plan ' + str(date_to_check) + ': ' + str(plan_row_count) + ' rows were added')

    if hour == 6:
        new_change_count = changes.count_entries(date_to_check)
        count_delta = new_change_count - old_change_count
        if count_delta < 50000:
            message = '''@everyone The recent change gatherer is not working, as {} 
                      new entries where added to database at {}'''\
                      .format(str(count_delta), str(date_to_check))
            await channel.send(message)
        old_change_count = new_change_count
        print('checked changes ' + str(date_to_check) + ': ' + str(count_delta) + ' rows were added')


class Monitor(commands.Cog):
    def __init__(self):
        self.monitor.start()

    def monitor_unload(self):
        self.monitor.cancel()

    @tasks.loop(hours=1)
    async def monitor(self):
        await client.wait_until_ready()
        await monitor_hour()


if __name__ == "__main__":
    import fancy_print_tcp
    # client.loop.create_task(monitor())
    m = Monitor()
    client.run(discord_bot_token)

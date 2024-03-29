# dcu-bots
Home for bots

## Table of Contents
  1. [Missed Ticket Bot](#missed-ticket-bot)
  2. [Database Backups](#database-backups)

API Bot
## Missed Ticket Bot

NOTE: Each script will look for an ```.ini``` file to read its settings from. The settings file should be named ```<script_name>_settings.ini``` and placed in the same directory as its parent script. Be sure to set the permissions on the ```.ini``` files appropriately so that no passwords are readable. Listed below are example ```.ini``` files for each of the scripts. Values surrounded with ```< >``` will need to be substituted with real values before runtime

### missed_tickets.py
#### missed_tickets_settings.ini

```
[DEFAULT]
slack_url = https://hooks.slack.com/services/<secret>/<secret>/<secret>

[prod]
db_url = mongodb://<127.0.0.1:27017/ >
db_user = <user>
db_pass = <pass>
db_auth_mechanism = <MONGODB-CR>
db = phishstory
snow_user = <snow_user>
snow_pass = <snow_pass>
snow_url = https://godaddy.service-now.com/api/now/table/u_dcu_ticket?sysparm_query=u_closed%%3Dfalse%%5E&sysparm_limit=20000
celery_task = run.process
celery_queue = dcumiddleware
broker_user = <b_user>
broker_pass = <b_pass>
broker_url = rmq-dcu.int.godaddy.com:5672/grandma
slack_channel = <#somechannel>


[dev]
db_url = mongodb://<127.0.0.1:27017/ >
db_user = <user>
db_pass = <pass>
db_auth_mechanism = <MONGODB-CR>
db = phishstory
snow_user = <snow_user>
snow_pass = <snow_pass>
snow_url = https://godaddy.service-now.com/api/now/table/u_dcu_ticket?sysparm_query=u_closed%%3Dfalse%%5E&sysparm_limit=20000
celery_task = run.process
celery_queue = dcumiddleware
broker_user = <b_user>
broker_pass = <b_pass>
broker_url = rmq-dcu.int.godaddy.com:5672/grandma
slack_channel = <#somechannel>
```

Each section above has the same keys defined, and the values should only differ based on the prod/dev environment.

### api_bot.py
#### api_bot_settings.ini
```
[slack]
slack_url = https://hooks.slack.com/services/<secret>/<secret>/<secret>

[prod]
endpoint = apibot
int_url = https://abuse.api.int.godaddy.com/v1/abuse/tickets/%(endpoint)s
ext_url = https://api.godaddy.com/v1/abuse/tickets/%(endpoint)s
jwt = <rawjwttoken>
key_secret = <secret:key>
sso_url = https://sso.godaddy.com

[ote]
endpoint = apibot
int_url = https://abuse.api.int.ote-godaddy.com/v1/abuse/tickets/%(endpoint)s
ext_url = https://api.ote-godaddy.com/v1/abuse/tickets/%(endpoint)s
jwt = <rawjwttoken>
key_secret = <secret:key>
sso_url = https://sso.ote-godaddy.com

[dev]
endpoint = apibot
int_url = https://abuse.api.int.dev-godaddy.com/v1/abuse/tickets/%(endpoint)s
ext_url = https://api.dev-godaddy.com/v1/abuse/tickets/%(endpoint)s
jwt = <rawjwttoken>
key_secret = <secret:key>
sso_url = https://sso.dev-godaddy.com
```
Three environments need to be defined in the above settings file. NOTE: all three environments have the same keys defined. Only the values will change based on the environment.

## Database Backups
Since DCU is responsible for backing up their own Mongo databases, we have backup scripts on the `bots-box` VM, which is also where the actual backed up files are located

There is one backup script responsible for backing up all databases, which is located at `/home/dcu-bots/db_backup/db_backup.sh`

### Kelvin Backups
The `/home/dcu-bots/db_backups/kelvin_backup_bot.ini` file contains the settings required to backup the Kelvin specific collections

The crontab for this backup is: `59  23  *  *  *     /home/dcu-bots/db_backup/db_backup.sh kelvin`

The backup files will be written to the `/home/db_backups/kelvin/` directory

### Phishstory Backups
The `/home/dcu-bots/db_backups/phishstory_backup_bot.ini` file contains the settings required to backup the PhishStory specific collections

The crontab for this backup is: `59  21  *  *  *     /home/dcu-bots/db_backup/db_backup.sh phishstory`

The backup files will be written to the `/home/db_backups/phishstory/` directory

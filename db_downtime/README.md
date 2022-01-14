# db-downtime
When DCU's MongoDB is taken offline for maintenance or upgrade, then tickets submitted to DCU's Abuse API are written to SNOW only.

When the DB is brought back online, then we must find the tickets which were created in SNOW and insert them into the DB.

[This document](https://confluence.godaddy.com/display/ITSecurity/Abuse+API+MongoDB+Upgrade+Readiness) details the moving parts which encompass this process

## Table of Contents
  1. [PhishStory](#phishstory)
  2. [Kelvin](#kelvin)
  3. [Configuration File](#configuration-file)

## PhishStory

This is where information regarding the PhishStory process will go.

## Kelvin

The `submit_snow_tickets_kelvin.py` file is responsible for creating Kelvin tickets in DCU's kelvin database, based on the values from newly created SNOW tickets.

### APIs

The Kelvin script reaches out to the _CMAP Service_ API, which requires using the `cmapservice.int` certificate/key in the `certs` directory.

### Running

Before running the script, ensure the value for `SNOWHelper.QUERY_TIME` is appropriate for your needs.
The QUERY_TIME variable should be set to a datetime prior to when the database was taken down.

Also ensure the [Configuration File](#configuration-file) has the appropriate configuration values set.

The output will be logged to the filename specified in the `logging.yaml` file.

To manually run the script, set a `sysenv` environment variable to `prod` and type `python submit_snow_tickets_kelvin.py` from the _legacy dcu-bots_ VM terminal.

## Configuration File

NOTE: The script responsible for processing for both PhishStory and Kelvin will look for an ```.ini``` file to read its settings from.
The settings file is named ```connection_settings.ini``` and placed in the same directory as its calling script.
Be sure to set the permissions on the ```.ini``` files appropriately so that no passwords are readable by anyone but its owner.
Listed below is an example of the ```connection_settings.ini``` file used by each of the scripts.
Values surrounded with ```< >``` will need to be substituted with real values prior to runtime.

### connection_settings.ini

```
[DEFAULT]
slack_url = <SLACK_URL>
celery_task = run.process
broker_user = <BROKER_USER>
broker_pass = <BROKER_PASS>
broker_url = rmq-dcu.int.godaddy.com:5672/grandma

[prod]
pdna_reporter_id = 222151473
#DB
db_url = <DB_URL>
db_auth_mechanism = MONGODB-CR
#PhishStory
phishstory_url=<mongo connection string for phishstory>
db = <DB_FOR_PHISHSTORY>
#Kelvin
kelvin_url=<mongo connection string for kelvin>
db_k = <DB_FOR_KELVIN>
#SNOW
snow_user = <SNOW_USER>
snow_pass = <SNOW_PASS>
snow_url = https://godaddy.service-now.com/api/now/table/u_dcu_ticket?sysparm_query=sys_created_on>=javascript:gs.dateGenerate({querytime})^sys_created_on<javascript:gs.dateGenerate({endtime})
snow_kelvin_url = https://godaddy.service-now.com/api/now/table/u_dcu_kelvin?sysparm_query=sys_created_on>=javascript:gs.dateGenerate({querytime})^sys_created_on<javascript:gs.dateGenerate({endtime})
#CELERY
celery_queue = <CELERY_QUEUE>
#SLACK
slack_channel = #dcu_alerts
#API
cmap_service_url = https://cmapservice.int.godaddy.com/graphql
cmap_service_cert = ./certs/cmapservice.int.godaddy.com.crt
cmap_service_key = ./certs/cmapservice.int.godaddy.com.key
sso_url = https://sso.godaddy.com

[dev]
pdna_reporter_id = 1767806
#DB
db_url = <DB_URL>
db_auth_mechanism = SCRAM-SHA-1
#PhishStory specific
phishstory_url=<mongo connection string for phishstory>
db = <DB_FOR_PHISHSTORY>
#Kelvin specific
kelvin_url=<mongo connection string for kelvin>
db_k = <DB_FOR_KELVIN>
#SNOW
snow_user = <SNOW_USER>
snow_pass = <SNOW_PASS>
snow_url = https://godaddy.service-now.com/api/now/table/u_dcu_ticket?sysparm_query=sys_created_on>=javascript:gs.dateGenerate({querytime})^sys_created_on<javascript:gs.dateGenerate({endtime})
snow_kelvin_url = https://godaddy.service-now.com/api/now/table/u_dcu_kelvin?sysparm_query=sys_created_on>=javascript:gs.dateGenerate({querytime})^sys_created_on<javascript:gs.dateGenerate({endtime})
#CELERY
celery_queue = <CELERY_QUEUE>
#SLACK
slack_channel = #queue_bot_test
#API
cmap_service_url = https://cmapservice.int.dev-godaddy.com/graphql
cmap_service_cert = ./certs/cmapservice.int.dev-godaddy.com.crt
cmap_service_key = ./certs/cmapservice.int.dev-godaddy.com.key
sso_url = https://sso.dev-godaddy.com
```
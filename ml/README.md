# ml
Scripts in the `ml` directory are specific to automation as it pertains in some way to machine learning

## Table of Contents
  1. [PhishStory](#phishstory)
  2. [Close Tickets As FP Based on ML](#close-tickets-as-fp-based-on-ml)
  3. [Configuration File](#configuration-file)

## Close Tickets As FP Based on ML

The `close_tickets_fp_based_on_ml` script will find all _phishing_ ticket ids which are _open_ and have a ML fraud_score
between 0.0 and 0.05, in the corresponding database.  It will then call the abuse API's PATCH endpoint with the ticket
id, which will close the ticket as False Positive.

This script will also create the `actions` sub-document in the Mongo record, if it doesnt exist, and populate with an
entry similar to:
```
"actions" : [ 
    {
        "origin" : "SOME-HOSTNAME:/Users/someuser/dcu-bots/ml/close_tickets_fp_based_on_ml.py:APIHelper:close_incident",
        "timestamp" : ISODate("2020-09-09T16:58:01.602Z"),
        "message" : "closed as false positive",
        "user" : "automation"
    }
]
```
More information can be found in [this document](https://confluence.godaddy.com/display/ITSecurity/Long+Term+Structure+of+Actions)

### APIs

This script calls the Abuse API PATCH endpoint using `dcu_middleware` credentials.

### Running

The `sysenv` environment variable should be set... specifically to `prod`, providing you want to run against the production environment.

Also ensure the [Configuration File](#configuration-file) have the appropriate configuration values set.

The output will be logged to the filename specified in the `logging.yaml` file.

To manually run the script, set a `sysenv` environment variable to `prod` and type `python close_tickets_fp_based_on_ml.py` from the _legacy dcu-bots_ VM terminal, in the appropriate directory...
HOWEVER, this script should be added to the CRON so that it is run daily.

#### Example Crontab
0 4 * * * export sysenv=prod;/usr/local/bin/python /home/dcu-bots/ml/close_tickets_fp_based_on_ml.py

## Configuration File

NOTE: The script will look for an ```.ini``` file to read its settings from.
The settings file is named ```connection_settings.ini``` and placed in the same directory as its calling script.
Be sure to set the permissions on the ```.ini``` files appropriately so that no passwords are readable by anyone but its owner.
Listed below is an example of the ```connection_settings.ini``` file used by each of the scripts.
Values surrounded with ```< >``` will need to be substituted with real values prior to runtime.

### connection_settings.ini

```
[prod]
#DB
db_url = <DB_URL>
db_auth_mechanism = MONGODB-CR
#PhishStory
db_user = <DB_USER_FOR_PHISHSTORY>
db_pass = <DB_PASS_FOR_PHISHSTORY>
db = <DB_FOR_PHISHSTORY>
#API
abuse_api = https://abuse.api.int.godaddy.com/v1/abuse/tickets
dcu_middlware_jwt = <JWT_FOR_DCU_MIDDLEWARE_USER>

[dev]
#DB
db_url = <DB_URL>
db_auth_mechanism = SCRAM-SHA-1
#PhishStory
db_user = <DB_USER_FOR_PHISHSTORY>
db_pass = <DB_PASS_FOR_PHISHSTORY>
db = <DB_FOR_PHISHSTORY>
#API
abuse_api = https://abuse.api.int.dev-godaddy.com/v1/abuse/tickets
dcu_middleware_jwt = <JWT_FOR_DCU_MIDDLEWARE_DEV_USER>
```
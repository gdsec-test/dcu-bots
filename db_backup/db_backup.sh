#!/bin/sh

####################################################################################
# The script is used to dump the collections of a mongo db in a date partioned folder
# We use the slack bash console script for sending messages to slack channel in case the script fails to dump the data
####################################################################################
# Installation of slack bash script
#    $ curl -s https://gist.githubusercontent.com/andkirby/67a774513215d7ba06384186dd441d9e/raw --output /usr/bin/slack
#    $ chmod +x /usr/bin/slack
####################################################################################
# USAGE of slack bash script
# Send message to slack channel/user
#   Send a message to the channel #ch-01
#     $ slack '#ch-01' 'Some message here.'
#
#   Send a message to the channel #ch-01 and user @me.
#     $ slack '#ch-01,@me' MESSAGE
#
#   Send a message to the default channel (it must be declared in APP_SLACK_CHANNEL).
#     $ slack  MESSAGE
#
# VARIABLES
#
# Please declare environment variables:
#   - APP_SLACK_WEBHOOK
#   - APP_SLACK_CHANNEL (optional)
#   - APP_SLACK_USERNAME (optional)
#   - APP_SLACK_ICON_EMOJI (optional)
# You may also declare them in ~/.slackrc file.
# We declared these variables in /etc/profile.d/slack.sh file
####################################################################################

#DBdump Script

# $1 is the first parameter passed in, which should be either "kelvin" or "phishstory". This tells the script which
#  ini file to source.
source "/home/dcu-bots/db_backup/$1_backup_bot.ini"
. /etc/profile.d/slack.sh

#Create the backup directory
mkdir -p $DEST/$DIR

#Dump the required collections in the backup directory - one by one
#We can even dump the entire database in one go

for collection in ${COLLECTIONS[@]}; do
    mongodump mongodb://$IP_ADDR:$PORT/$DB -u $USERNAME -p $PASSWORD --collection=$collection -o $DEST/$DIR
    if [ $? -ne 0 ]; then
   	msg="Backup failed for the $collection collection of $DB database"
    	echo $msg
    	slack $msg
    fi
done

find $DEST/* -type d -ctime +5 | xargs rm -rf

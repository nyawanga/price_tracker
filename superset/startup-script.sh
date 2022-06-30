#!/usr/bin/env bash

superset fab create-admin \
--username ${USERNAME} \
--firstname ${FIRST_NAME} \
--lastname ${LAST_NAME} \
--email ${EMAIL} \
--password ${PASSWORD} 

superset db upgrade
superset init

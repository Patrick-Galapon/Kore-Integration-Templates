###
# Title:    tickets_template.py
# Author:   Patrick Galapon
# Date:     2022-08-11
# Version:  1.4
#
# A template for the Kore API Integration into Eloqua, 
# Exports and parses data from the S3 directory set up by Kore and imports into a Tickets CDO in Eloqua.
# To be used for the basis of future Kore API Integation implementations of the Tickets entity.
#
# UPDATES
#
#
# Version 1.4:
# - updated to Python3 syntax
#
# Version 1.3:
# - updated the email notificationality to use mailgun (rather than google api)
#
# Version 1.2:
# - inserted portion to write reporting data to a database using pyodbc library
# - inserted email notification process used to send the reports data to recipient(s)
#
# Version 1.1:
# - added core functionality of exporting and parsing data from S3 and imports into Tickets CDO
#
###

import sys, os
import boto
from boto.s3.connection import S3Connection
import sys, os
from boto.s3.key import Key
import urllib.request
import csv
import codecs
import requests
import json
import base64
import time
import datetime
import pyodbc



#Sync function for retreiving exports/imports from Eloqua
def sync(uri, response, base_url):
    if response.json()['status'] == 'pending':
        syncrequest = requests.get(base_url + '/api/bulk/2.0' + uri, headers=headers)
        if syncrequest.json()['status'] == 'success':
            return True
        elif syncrequest.json()['status'] == 'pending':
            print('Sync Pending')
            sync(uri, response, base_url)
        elif syncrequest.json()['status'] == 'active':
            print('Sync Churning')
            sync(uri, response, base_url)
        elif syncrequest.json()['status'] == 'warning' or 'error':
            print((requests.get(base_url + '/api/bulk/2.0' + uri + '/logs', headers=headers)).json())
        else:
            print(syncrequest.json())
            print('Sync Failed')



def getEloquaAccess():

    # Assign login variables
    site = ''
    username = ''
    pw = ''

    # Set up authentication encoding  
    key = site+'\\'+username+':'+pw
    authKey = base64.b64encode(site + '\\' + username + ':' + pw)

    # Key and JSON signals to Eloqua
    headers = {
        "Authorization": "Basic " + authKey.strip(),
        "content-type":"application/json",
        "Accept":"application/json; encoding='utf8'"
    }

    r = requests.get('https://login.eloqua.com/id', headers=headers)
    baseUrl = r.json()['urls']['base']

    apiAccess = {}
    apiAccess['sitename'] = site
    apiAccess['headers'] = headers
    apiAccess['baseUrl'] = baseUrl
    
    return apiAccess



def getBucketInfo():

    # Assign Access Keys provided by Kore
    AWS_ACCESS_KEY_ID = ''
    AWS_SECRET_ACCESS_KEY = ''

    # Assign S3 directory of the Membersip file(s) provided by Kore
    bucket_name = ''
    prefix = ''

    bucketInfo = {}
    bucketInfo['accessKey'] = AWS_ACCESS_KEY_ID
    bucketInfo['secretAccessKey'] = AWS_SECRET_ACCESS_KEY
    bucketInfo['bucketName'] = bucket_name
    bucketInfo['prefix'] = prefix

    return bucketInfo



def getTicketIntegrationSummary():                

    apiAccess = getEloquaAccess()
    sitename = apiAccess['site']
    headers = apiAccess['headers']
    baseUrl = apiAccess['baseUrl']

    print('Retrieving Tickets CDO Data Created Today...')

    cdoData = json.dumps({
        "name" : "Retrieve SYSTEM - KORE Tickets CDO Data Created Today",
        "fields" : {
            "pk" : "{{CustomObject[<customDataObjectId>].Field[<fieldId>]}}",
            "emailAddress" : "{{CustomObject[<customDataObjectId>].Field[<fieldId>]}}",
            "createDate" : "{{CustomObject[<customDataObjectId>].CreatedAt}}",
            "updateDate" : "{{CustomObject[<customDataObjectId>].UpdatedAt}}"
        },
        "filter" : "'{{CustomObject[<customDataObjectId>].CreatedAt}}' >= '" + todayDate + "'"
    })

    cdoUrl = baseUrl + '/api/bulk/2.0/customObjects/<customDataObjectId>/exports'
    cdoReqExport = requests.post(cdoUrl, headers=headers, data=cdoData)

    cdoExportUri = cdoReqExport.json()["uri"]

    # Setup sync instance 
    cdoReqExportSyncData = json.dumps({
        "syncedInstanceUri" : cdoExportUri}
    )

    # Sync the data retrieval
    cdoReqSync = requests.post(baseUrl + '/api/bulk/2.0/syncs', headers=headers, data=cdoReqExportSyncData)

    if cdoReqSync.status_code == 201:
        cdoSyncUri = cdoReqSync.json()['uri']
        sync(cdoSyncUri, cdoReqSync, bulkUrl)
    else: 
        print('cdoReqSync status code failed to process...')
        #quit()

    # Get the Sync Instance URI
    cdoReqSyncInstanceUri = cdoReqSync.json()['syncedInstanceUri']

    # Uxing the Sync Istance URI, retrieve the data from Eloqua API
    # Keep trying to retrieve the data until successful
    cdoCount = 0
    cdoHasMore = 'False'

    cdoReqData = requests.get(baseUrl + '/api/bulk/2.0' + cdoReqSyncInstanceUri + '/data', params='limit=50000', headers=headers)
    #parsedResponse = r.json()
    cdoReqDataCount = int(cdoReqData.json()['count'])

    print('Retrieving first 50000 CDO records...')
    ticketDataCreated = []

    if cdoReqDataCount > 0:
        cdoReqDataItems = cdoReqData.json()['items']
    else:
        cdoReqDataItems = []

    for element in cdoReqDataItems:
        elem = {}
        elem['pk'] = element['pk']
        elem['emailAddress'] = element['emailAddress'].lower()
        elem['createDate'] = element['createDate']
        elem['updateDate'] = element['updateDate']
        ticketDataCreated.append(elem)

    cdoHasMore = cdoReqData.json()["hasMore"]
    cdoOffsetNum = 0
    #orig_url = url

    while cdoHasMore is True:
        cdoOffsetNum = cdoOffsetNum + 50000
        print('Retrieving next {} CDO records...'.format(cdoOffsetNum))
        cdoReqUrl = bulkUrl + cdoReqSyncInstanceUri + '/data' + '?offset=%d' % cdoOffsetNum

        cdoReqData = requests.get(cdoReqUrl, params='limit=50000', headers=headers)
        cdoReqDataCount = int(cdoReqData.json()['count'])

        if cdoReqDataCount > 0:
            cdoReqDataItems = cdoReqData.json()['items']
        else:
            cdoReqDataItems = []

        for element in cdoReqDataItems:
            elem = {}
            elem['pk'] = element['pk']
            elem['emailAddress'] = element['emailAddress'].lower()
            elem['createDate'] = element['createDate']
            elem['updateDate'] = element['updateDate']
            ticketDataCreated.append(elem)
        
        cdoHasMore = cdoReqData.json()["hasMore"]
      
    print('Number of Ticket Data Created : {}'.format(len(ticketDataCreated)))

    print('Retrieving Tickets CDO Data Updated Today...')

    cdoData = json.dumps({
        "name" : "Retrieve SYSTEM - KORE Tickets CDO Data Updated Today",
        "fields" : {
            "pk" : "{{CustomObject[<customDataObjectId>].Field[<fieldId>]}}",
            "emailAddress" : "{{CustomObject[<customDataObjectId>].Field[<fieldId>]}}",
            "createDate" : "{{CustomObject[<customDataObjectId>].CreatedAt}}",
            "updateDate" : "{{CustomObject[<customDataObjectId>].UpdatedAt}}"
        },
        "filter" : "'{{CustomObject[<customDataObjectId>].UpdatedAt}}' >= '" + todayDate + "' AND '{{CustomObject[<customDataObjectId>].CreatedAt}}' < '" + todayDate + "'"
    })

    cdoUrl = baseUrl + '/api/bulk/2.0/customObjects/<customDataObjectId>/exports'
    cdoReqExport = requests.post(cdoUrl, headers=headers, data=cdoData)

    cdoExportUri = cdoReqExport.json()["uri"]

    # Setup sync instance 
    cdoReqExportSyncData = json.dumps({
        "syncedInstanceUri" : cdoExportUri}
    )

    # Sync the data retrieval
    cdoReqSync = requests.post(baseUrl + '/api/bulk/2.0/syncs', headers=headers, data=cdoReqExportSyncData)

    if cdoReqSync.status_code == 201:
        cdoSyncUri = cdoReqSync.json()['uri']
        sync(cdoSyncUri, cdoReqSync, bulkUrl)
    else: 
        print 'cdoReqSync status code failed to process...'
        #quit()

    # Get the Sync Instance URI
    cdoReqSyncInstanceUri = cdoReqSync.json()["syncedInstanceUri"]

    # Uxing the Sync Istance URI, retrieve the data from Eloqua API
    # Keep trying to retrieve the data until successful
    cdoCount = 0
    cdoHasMore = "False"

    cdoReqData = requests.get(baseUrl + '/api/bulk/2.0' + cdoReqSyncInstanceUri + '/data', params='limit=50000', headers=headers)
    cdoReqDataCount = int(cdoReqData.json()['count'])

    print('Retrieving first 50000 CDO records...')
    ticketDataUpdated = []

    if cdoReqDataCount > 0:
        cdoReqDataItems = cdoReqData.json()['items']
    else:
        cdoReqDataItems = []

    for element in cdoReqDataItems:
        elem = {}
        elem['pk'] = element['pk']
        elem['emailAddress'] = element['emailAddress'].lower()
        elem['createDate'] = element['createDate']
        elem['updateDate'] = element['updateDate']
        ticketDataUpdated.append(elem)

    cdoHasMore = cdoReqData.json()["hasMore"]
    cdoOffsetNum = 0

    while cdoHasMore is True:
        cdoOffsetNum = cdoOffsetNum + 50000
        print('Retrieving next {} CDO records...'.format(cdoOffsetNum))
        cdoReqUrl = bulkUrl + cdoReqSyncInstanceUri + '/data' + '?offset=%d' % cdoOffsetNum

        cdoReqData = requests.get(cdoReqUrl, params='limit=50000', headers=headers)
        cdoReqDataCount = int(cdoReqData.json()['count'])

        if cdoReqDataCount > 0:
            cdoReqDataItems = cdoReqData.json()['items']
        else:
            cdoReqDataItems = []

        for element in cdoReqDataItems:
            elem = {}
            elem['pk'] = element['pk']
            elem['emailAddress'] = element['emailAddress'].lower()
            elem['createDate'] = element['createDate']
            elem['updateDate'] = element['updateDate']
            ticketDataUpdated.append(elem)
        
        cdoHasMore = cdoReqData.json()["hasMore"]
      
    print('Number of Ticket Data Updated : {}'.format(len(ticketDataUpdated)))

    ticketIntegrationSummary = {}
    ticketIntegrationSummary['dataCreated'] = len(ticketDataCreated)
    ticketIntegrationSummary['dataUpdated'] = len(ticketDataUpdated)

    return ticketIntegrationSummary



def startTicketIntegration(clientName):

    apiAccess = getEloquaAccess()
    sitename = apiAccess['site']
    headers = apiAccess['headers']
    baseUrl = apiAccess['baseUrl']

    successFlag = True
    numOfRetries = 1
    actualRetries = 1

    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')

    print('\n')
    print('Started running {} - Tickets.py : {}'.format(clientName, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    while numOfRetries <= 5:

        print('Running script {} of 5 times...'.format(numOfRetries))

        try:

            print('Retrieving all Email Addresses ' + sitename + ' in Instance...')
            reqExportData = json.dumps({
                'name' : 'All Email Addresses',
                'fields' : {
                    'emailAddress' : '{{Contact.Field(C_EmailAddress)}}',
                    },
                })

            reqExport = requests.post(baseUrl + '/api/bulk/2.0/contacts/exports', headers=headers, data=reqExportData)

            reqExportUri = reqExport.json()["uri"]

            # Setup sync instance 
            reqSyncData = json.dumps({
                'syncedInstanceUri' : reqExportUri}
                )

            # Sync the data retrieval
            reqSync = requests.post(baseUrl + '/api/bulk/2.0/syncs', headers=headers, data=reqSyncData)

            if reqSync.status_code == 201:
                syncUri = reqSync.json()['uri']
                sync(syncUri, reqSync, bulkUrl)
            else: 
                print('Failed to sync...')
                #quit()

            # Get the Sync Instance URI
            reqSyncInstanceUri = reqSync.json()['syncedInstanceUri']

            # Uxing the Sync Istance URI, retrieve the data from Eloqua API
            # Keep trying to retrieve the data until successful
            reqDataUrl = baseUrl + '/api/bulk/2.0' + reqSyncInstanceUri + '/data'
            count = 0
            hasMore = 'False'

            reqData = requests.get(reqDataUrl, params='limit=50000', headers=headers)

            print('Retrieving first 50000 records...')
            emailAddress = []
            for element in reqData.json()['items']:
                emailAddress.append(element['emailAddress'].lower())

            hasMore = reqData.json()['hasMore']
            offsetNum = 0

            while hasMore is True:
                offsetNum = offsetNum + 50000
                print('Retrieving next {} records...'.format(offsetNum))
                reqDataUrl = baseUrl + '/api/bulk/2.0' + reqSyncInstanceUri + '/data' + '?offset=%d' % offsetNum

                reqData = requests.get(reqDataUrl, params='limit=50000', headers=headers)

                for element in reqData.json()['items']:
                    emailAddress.append(element['emailAddress'].lower())
                
                hasMore = reqData.json()['hasMore']
              
            print('Number of Email Addresses : {}'.format(len(emailAddress)))

            print('Setting up Email Address Set...')
            emailAddressSet = set(emailAddress)

            print('Processing KORE files for the ' + clietName)

            # Connect to the bucket
            conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
                            AWS_SECRET_ACCESS_KEY)
            bucket = conn.get_bucket(bucket_name, validate=False)
            # go through the list of files
            bucket_list = bucket.list(prefix=prefix)

            keyStringArray = []
            for l in bucket_list:
                if 'ticket_' in str(l.key):
                    keyString = str(l.key)
                    print('Keystring : {}'.format(str(keyString)))
                    keyStringArray.append(keyString)

            fileNum = 1
            for keyString in keyStringArray:
                filename = keyString.split('/')[-1]
                print('Filename : {}'.format(str(filename)))

                key=bucket.get_key(keyString)
                print('Key : {}'.format(str(key)))

                url = conn.generate_url(
                    60,
                    'GET',
                    bucket_name,
                    keyString
                    )

                bucketData = urllib.request.urlopen(url)
                contentOrig = []
                for line in bucketData:
                    contentOrig.append(line.decode('utf-8'))

                print('Number of records to process : {}'.format(len(contentOrig)))

                #continue 

                exportData = []
                counter = 0
                fieldnameFlag = 0
                for i in contentOrig:

                    if counter == 0:
                        counter = counter + 1
                        continue

                    contentSplit = i.split('|')
                    try:
                        elem = {}
                        elem['pk'] = contentSplit[0]
                        elem['emailAddress'] = contentSplit[1]
                        elem['seatGroup'] = contentSplit[2]
                        elem['acctId'] = contentSplit[3]
                        elem['seasonName'] = contentSplit[4]
                        elem['eventId'] = contentSplit[5]
                        elem['tmEventName'] = contentSplit[6]
                        elem['eventName'] = contentSplit[7]
                        elem['eventNameLong'] = contentSplit[8]
                        elem['sectionName'] = contentSplit[9]
                        elem['rowName'] = contentSplit[10]
                        elem['priceCode'] = contentSplit[11]
                        elem['purchasePrice'] = contentSplit[12]
                        elem['percentPaid'] = contentSplit[13]
                        elem['name'] = contentSplit[14]
                        elem['ticketStatus'] = contentSplit[15]
                        elem['groupFlag'] = contentSplit[16]
                        elem['acctRepFullName'] = contentSplit[17]
                        elem['addDateTime'] = contentSplit[18]
                        elem['seatIncrement'] = contentSplit[19]
                        elem['totalEvents'] = contentSplit[20]
                        elem['addUser'] = contentSplit[21]
                        elem['updDateTime'] = contentSplit[22]
                        elem['orderNum'] = contentSplit[23]
                        elem['orderLineItem'] = contentSplit[24]
                        elem['orderLineItemSeq'] = contentSplit[25]
                        elem['salesSourceName'] = contentSplit[26]
                        elem['nameLastFirstMi'] = contentSplit[27]
                        elem['seasonYear'] = contentSplit[28]
                        elem['compName'] = contentSplit[29]
                        elem['koreUpdated'] = contentSplit[30]
                        elem['fullPrice'] = contentSplit[31]
                        elem['printedPrice'] = contentSplit[32]
                        elem['seatNum'] = contentSplit[33]
                        elem['lastSeat'] = contentSplit[34]
                        elem['numSeats'] = contentSplit[35]
                        elem['seats'] = contentSplit[36]
                        elem['blockPurchasePrice'] = contentSplit[37]
                        elem['owedAmount'] = contentSplit[38]
                        elem['paidAmount'] = contentSplit[39]
                        elem['source'] = contentSplit[40]
                        elem['planEventName'] = contentSplit[41]
                        elem['priceCodeGroup'] = contentSplit[42]
                        elem['eventTypeCode'] = contentSplit[43]
                        elem['acctRepId'] = contentSplit[44]
                        elem['acctTypeDesc'] = contentSplit[45]
                        elem['otherInfo1'] = contentSplit[46]
                        elem['otherInfo2'] = contentSplit[47]
                        elem['otherInfo3'] = contentSplit[48]
                        elem['otherInfo4'] = contentSplit[49]
                        elem['otherInfo5'] = contentSplit[50]
                        elem['otherInfo6'] = contentSplit[51]
                        elem['otherInfo7'] = contentSplit[52]
                        elem['otherInfo8'] = contentSplit[53]
                        elem['otherInfo9'] = contentSplit[54]
                        elem['otherInfo10'] = contentSplit[55]
                        elem['ticketTypeCategory'] = contentSplit[56]
                        elem['databaseId'] = contentSplit[57]
                        elem['ledgerCode'] = contentSplit[58]
                        elem['seasonId'] = contentSplit[59]
                        elem['team'] = contentSplit[60]
                        elem['eventTime'] = contentSplit[61]
                        elem['className'] = contentSplit[62]
                        elem['pcTicket'] = contentSplit[63]
                        elem['pcTax'] = contentSplit[64]
                        elem['pcLicFee'] = contentSplit[65]
                        elem['ticketType'] = contentSplit[66]
                        elem['ticketTypeCode'] = contentSplit[67].replace('\n','')
                    except:
                        continue

                    if elem['emailAddress'].lower() in emailAddressSet:
                        exportData.append(elem)

                print('Number of records to import : {}'.format(len(exportData)))

                fileNum = fileNum + 1
                content_array = [exportData[x:x+10000] for x in xrange(0, len(exportData), 10000)]
                importCounter = 1

                print('Length of content_array : {}'.format(len(content_array)))

                for i in content_array:

                    importContents = []
                    for j in i:
                        tmp_contents = {
                            "pk" : j['pk'],
                            "email_addr" : j['emailAddress'],
                            "seatgroup" : j['seatGroup'],
                            "acct_id" : j['acctId'],
                            "season_name" : j['seasonName'],
                            "event_id" : j['eventId'],
                            "tm_event_name" : ['tmEventName'],
                            "event_name" : j['eventName'],
                            "event_name_long" : j['eventNameLong'],
                            "section_name" : j['sectionName'],
                            "row_name" : j['rowName'],
                            "price_code" : j['priceCode'],
                            "purchase_price" : j['purchasePrice'],
                            "percent_paid" : j['percentPaid'],
                            "name" : j['name'],
                            "ticket_status" : j['ticketStatus'],
                            "group_flag" : j['groupFlag'],
                            "acct_rep_full_name" : j['acctRepFullName'],
                            "add_datetime" : j['addDateTime'],
                            "seat_increment" : j['seatIncrement'],
                            "total_events" : j['totalEvents'],
                            "add_usr" : j['addUser'],
                            "upd_datetime" : j['updDateTime'],
                            "order_num" : j['orderNum'],
                            "order_line_item" : j['orderLineItem'],
                            "order_line_item_seq" : j['orderLineItemSeq'],
                            "sales_source_name" : j['salesSourceName'],
                            "name_last_first_mi" : j['nameLastFirstMi'],
                            "season_year" : j['seasonYear'],
                            "comp_name" : j['compName'],
                            "kore_updated" : j['koreUpdated'],
                            "full_price" : j['fullPrice'],
                            "printed_price" : j['printedPrice'],
                            "seat_num" : j['seatNum'],
                            "last_seat" : j['lastSeat'],
                            "num_seats" : j['numSeats'],
                            "seats" : j['seats'],
                            "block_purchase_price" : j['blockPurchasePrice'],
                            "owed_amount" : j['owedAmount'],
                            "paid_amount" : j['paidAmount'],
                            "source" : j['source'],
                            "plan_event_name" : j['planEventName'],
                            "price_code_group" : j['priceCodeGroup'],
                            "event_type_code" : j['eventTypeCode'],
                            "acct_rep_id" : j['acctRepId'],
                            "acct_type_desc" : j['acctTypeDesc'],
                            "other_info_1" : j['otherInfo1'],
                            "other_info_2" : j['otherInfo2'],
                            "other_info_3" : j['otherInfo3'],
                            "other_info_4" : j['otherInfo4'],
                            "other_info_5" : j['otherInfo5'],
                            "other_info_6" : j['otherInfo6'],
                            "other_Info_7" : j['otherInfo7'],
                            "other_info_8" : j['otherInfo8'],
                            "other_info_9" : j['otherInfo9'],
                            "other_info_10" : j['otherInfo10'],
                            "ticket_type_category" : j['ticketTypeCategory'],
                            "database_id" : j['databaseId'],
                            "ledger_code" : j['ledgerCode'],
                            "season_id" : j['seasonId'],
                            "team" : j['team'],
                            "event_time" : j['eventTime'],
                            "class_name" : j['className'],
                            "pc_ticket" : j['pcTicket'],
                            "pc_tax" : j['pcTax'],
                            "pc_licfee" : j['pcLicFee'],
                            "ticket_type" : j['ticketType'],
                            "ticket_type_code" : j['ticketTypeCode']
                            }

                        importContents.append(tmp_contents)

                    print('Import records into CDO {} of {}...'.format(importCounter, len(content_array)))
                    print('Number of records to import: {}'.format(len(importContents)))

                    data = {
                        "name": "SYSTEM - KORE Tickets CDO Import",
                        "fields": {
                            "pk" : "{{CustomObject[15].Field[234]}}",
                            "email_addr" : "{{CustomObject[15].Field[235]}}",
                            "seatgroup" : "{{CustomObject[15].Field[236]}}",
                            "acct_id" : "{{CustomObject[15].Field[237]}}",
                            "season_name" : "{{CustomObject[15].Field[238]}}",
                            "event_id" : "{{CustomObject[15].Field[239]}}",
                            "tm_event_name" : "{{CustomObject[15].Field[240]}}",
                            "event_name" : "{{CustomObject[15].Field[241]}}",
                            "event_name_long" : "{{CustomObject[15].Field[301]}}",
                            "section_name" : "{{CustomObject[15].Field[242]}}",
                            "row_name" : "{{CustomObject[15].Field[243]}}",
                            "price_code" : "{{CustomObject[15].Field[244]}}",
                            "purchase_price" : "{{CustomObject[15].Field[245]}}",
                            "percent_paid" : "{{CustomObject[15].Field[246]}}",
                            "name" : "{{CustomObject[15].Field[247]}}",
                            "ticket_status" : "{{CustomObject[15].Field[248]}}",
                            "group_flag" : "{{CustomObject[15].Field[249]}}",
                            "acct_rep_full_name" : "{{CustomObject[15].Field[250]}}",
                            "seat_increment" : "{{CustomObject[15].Field[252]}}",
                            "total_events" : "{{CustomObject[15].Field[253]}}",
                            "add_usr" : "{{CustomObject[15].Field[254]}}",
                            "upd_datetime" : "{{CustomObject[15].Field[255]}}",
                            "order_num" : "{{CustomObject[15].Field[256]}}",
                            "order_line_item" : "{{CustomObject[15].Field[257]}}",
                            "order_line_item_seq" : "{{CustomObject[15].Field[258]}}",
                            "add_datetime" : "{{CustomObject[15].Field[251]}}",
                            "sales_source_name" : "{{CustomObject[15].Field[259]}}",
                            "name_last_first_mi" : "{{CustomObject[15].Field[260]}}",
                            "season_year" : "{{CustomObject[15].Field[261]}}",
                            "comp_name" : "{{CustomObject[15].Field[262]}}",
                            "kore_updated" : "{{CustomObject[15].Field[263]}}",
                            "full_price" : "{{CustomObject[15].Field[264]}}",
                            "printed_price" : "{{CustomObject[15].Field[265]}}",
                            "seat_num" : "{{CustomObject[15].Field[266]}}",
                            "last_seat" : "{{CustomObject[15].Field[267]}}",
                            "num_seats" : "{{CustomObject[15].Field[268]}}",
                            "seats" : "{{CustomObject[15].Field[269]}}",
                            "block_purchase_price" : "{{CustomObject[15].Field[270]}}",
                            "owed_amount" : "{{CustomObject[15].Field[271]}}",
                            "paid_amount" : "{{CustomObject[15].Field[272]}}",
                            "source" : "{{CustomObject[15].Field[273]}}",
                            "plan_event_name" : "{{CustomObject[15].Field[274]}}",
                            "price_code_group" : "{{CustomObject[15].Field[275]}}",
                            "event_type_code" : "{{CustomObject[15].Field[276]}}",
                            "acct_rep_id" : "{{CustomObject[15].Field[277]}}",
                            "acct_type_desc" : "{{CustomObject[15].Field[278]}}",
                            "other_info_1" : "{{CustomObject[15].Field[279]}}",
                            "other_info_2" : "{{CustomObject[15].Field[280]}}",
                            "other_info_3" : "{{CustomObject[15].Field[281]}}",
                            "other_info_4" : "{{CustomObject[15].Field[282]}}",
                            "other_info_5" : "{{CustomObject[15].Field[283]}}",
                            "other_info_6" : "{{CustomObject[15].Field[284]}}",
                            "other_info_7" : "{{CustomObject[15].Field[285]}}",
                            "other_info_8" : "{{CustomObject[15].Field[286]}}",
                            "other_info_9" : "{{CustomObject[15].Field[287]}}",
                            "other_info_10" : "{{CustomObject[15].Field[288]}}",
                            "ticket_type_category" : "{{CustomObject[15].Field[289]}}", 
                            "database_id" : "{{CustomObject[15].Field[290]}}",
                            "ledger_code" : "{{CustomObject[15].Field[291]}}",
                            "season_id" : "{{CustomObject[15].Field[292]}}",
                            "team" : "{{CustomObject[15].Field[293]}}", 
                            "event_time" : "{{CustomObject[15].Field[294]}}",
                            "class_name" : "{{CustomObject[15].Field[295]}}",
                            "pc_ticket" : "{{CustomObject[15].Field[296]}}",
                            "pc_tax" : "{{CustomObject[15].Field[297]}}",
                            "pc_licfee" : "{{CustomObject[15].Field[298]}}",
                            "ticket_type" : "{{CustomObject[15].Field[299]}}",
                            "ticket_type_code" : "{{CustomObject[15].Field[300]}}"
                            },
                        "identifierFieldName" : "pk",
                        "isSyncTriggeredOnImport" : "true",
                        "mapDataCards" : "true",
                        "mapDataCardsEntityType" : "Contact",
                        "mapDataCardsSourceField": "email_addr",
                        "mapDataCardsEntityField" : "{{Contact.Field(C_EmailAddress)}}",
                        }

                     # To String
                    data = json.dumps(data)

                    importAttempt = 1

                    # NOTE - Uncomment the sections between the IMPORT lines to allow of import of Kore Membership file contents
                    #        into Ticket Activity CDO
                    # ########## IMPORT
                    # while importAttempt < 6:
                    #     print('Tickets_Daily Import Attempt ' + str(importAttempt) + ' of 5')
                
                    #     try:
                    #         # Post to create Export
                    #         r = requests.post(bulkUrl+'/customObjects/<customDataObjectId>/imports', data=data, headers=headers)
                    #         importuri = r.json()['uri'] #retrieve uri of import

                    #         # Push data to staging area
                    #         import_contents = json.dumps(importContents)
                    #         importResponse = requests.post(bulkUrl+importuri+'/data', data=import_contents, headers=headers)

                    #         #Execute Sync with import uri
                    #         data2 = {
                    #           'syncedInstanceUri' : importuri
                    #         }
                    #         data2 = json.dumps(data2)

                    #         r = requests.post(bulkUrl+'/syncs', data=data2, headers=headers)

                    #         importAttempt = 7

                    #         print('Import Successful.')
                    #     except:
                    #         print('Import Unsuccessful.')
                    #         importAttempt = importAttempt + 1
                    #         time.sleep(60)
                    # importCounter += 1
                    # ########## IMPORT

            ticketIntegrationSummary = getTicketIntegrationSummary()

            successFlag = True

            ticketsIntegrationSummary = getTicketIntegrationSummary()
            ticketsDataCreated = ticketsIntegrationSummary['dataCreated']
            ticketsDataUpdated = ticketsIntegrationSummary['dataUpdated']
            writeToTicketSummaryTable(successFlag, clientName, ticketsDataCreated, ticketsDataUpdated)

            numOfRetries = 7

        except Exception as e:

            print(e)
            print('Failed to run.')

            numOfRetries += 1
            actualRetries += 1

            successFlag = False            
            writeToTicketSummaryTable(successFlag, clientName, '0', '0')

            time.sleep(300)

    return successFlag



def writeToTicketSummaryTable(sFlag, clientName, dataCreated, dataUpdated)

    db = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',server='',database='', uid='', pwd='')

    # Set up database cursor
    c = db.cursor()

    todayDatetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if sFlag:
        sqlStmt = """
            insert into <clientName>_Tickets_Summary values (
                convert(datetime, '{}', 120), 'Success', {}, {}, 0
            )
            """.format(todayDatetime, len(dataCreated), len(dataUpdated))
        c.execute(sqlStmt)

    else:
        sqlStmt = """
            insert into <clientName>_Tickets_Summary values (
                convert(datetime, '{}', 120), 'Failed', 0, 0, 0
            )
            """.format(todayDatetime)

        #print sqlStmt
        c.execute(sqlStmt)

    db.commit()

    c.close()
    db.close()

    sendNotificationEmal(sFlag, clientName, dataCreated, dataUpdated)



def sendSuccessfulEmail(sFlag, clientName, dataCreated, dataUpdated):

    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')

    if sFlag:

        print('Generating Successful Notification Email...')

        successEmailSubjectLine = clientName + ' - KORE Integration - Tickets Successful for ' + todayDate
        successEmailSendTo = ''

        successEmailHtmlMessage = """
            <!DOCTYPE html PUBLIC '-//W3C//DTD XHTML 1.0 Transitional//EN' 'http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd'><html xmlns='http://www.w3.org/1999/xhtml'><head>
            <meta http-equiv='Content-Type' content='text/html; charset=utf-8'>
            <!--[if !mso]><!-->
            <meta http-equiv='X-UA-Compatible' content='IE=edge'>
            <!--<![endif]-->
            <meta name='viewport' content='width=device-width, initial-scale=1.0'>
            <title></title>
            <style type='text/css'>

            </style>
            <!--[if (gte mso 9)|(IE)]>
            <style type='text/css'>
            table {border-collapse: collapse !important;}
            </style>
            <![endif]-->
            <style type='text/css'>

            body {
            margin: 0 !important;
            padding: 0;
            background-color: #ffffff;
            }
            table {
            border-spacing: 0;
            font-family: Arial, Helvetica, sans-serif;
            color: #000000;
            }
            td {
            padding: 0;
            }
            img {
            border: 0;
            }
            div[style*='margin: 16px 0'] { 
            margin:0 !important;
            }
            .wrapper {
            width: 100%;
            table-layout: fixed;
            -webkit-text-size-adjust: 100%;
            -ms-text-size-adjust: 100%;
            }
            .webkit {
            max-width: 600px;
            margin: 0 auto;
            }
            .outer {
            margin: 0 auto;
            width: 100%;
            max-width: 600px;
            }
            .inner {
            padding: 10px;
            }
            .contents {
            width: 100%;
            }
            p, span {
            margin: 0;
            }
            a {
            color: #000000;
            text-decoration: underline;
            }

            .one-column .contents {
            text-align: left;
            }
            .one-column p {
            font-size: 14px;
            margin-bottom: 10px;
            }

            </style>
            </head>
            <body style='padding-top:0;padding-bottom:0;padding-right:0;padding-left:0;margin-top:0 !important;margin-bottom:0 !important;margin-right:0 !important;margin-left:0 !important;'>
            <center class='wrapper' style='width:100%;table-layout:fixed;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;'>
            <div class='webkit' style='max-width:600px;margin-top:0;margin-bottom:0;margin-right:auto;margin-left:auto;'>
              <!--[if (gte mso 9)|(IE)]>
            <table height='100%' width='100%' cellpadding='0' cellspacing='0' border='0'>
            <tr>
              <td valign='top' align='left' background=''>
              <table width='600' align='center' style='border-spacing:0;font-family:Arial, Helvetica, sans-serif;color:#000000;' >
              <tr>
              <td style='padding-top:0;padding-bottom:0;padding-right:0;padding-left:0;' >
              <![endif]-->
              <table bgcolor='#ffffff' cellpadding='0' cellspacing='0' border='0' class='outer' align='center' style='border-spacing:0;font-family:Arial, Helvetica, sans-serif;color:#000000;margin-top:0;margin-bottom:0;margin-right:auto;margin-left:auto;width:100%;max-width:600px;'>
                <tbody>
                  <tr>
                    <td class='one-column' style='padding-top:0;padding-bottom:0;padding-right:0;padding-left:0'>
                      <table cellpadding='0' cellspacing='0' border='0' width='100%'>
                        <tbody><tr>
                          <td class='inner contents' style='padding-left:30px;padding-right:30px;padding-top:10px;padding-bottom:10px;font-family:Avenir TT Book, Arial, Helvetica, sans-serif;font-size:16px;color:#000000;width:100%;text-align:left;'>
                            
                            <p>
                            This is to inform the recipient(s) that the """ + clientName + """ - KORE Integration for Tickets has successfully executed for """ + todayDate + """.
                            <br><br>
                            Records created: """ + str(len(dataCreated)) + """<br>
                            Records updated: """ + str(len(dataUpdated)) + """<br>
                            </p>
                          </td>
                        </tr>
                      </tbody></table>
                    </td>
                  </tr>
                </tbody>
              </table>
              <!--[if (gte mso 9)|(IE)]>
              </td>
              </tr>
              </table>
            <![endif]-->
            </div>
            </center>

            </body>
            </html>"""

        successEmailPlainMessage = ''

        print(successEmailSubjectLine)
        print('Sending Successful Email...')

        MAILGUN_API_KEY = ''
        MAILGUN_DOMAIN_NAME = ''

        requests.post(
            MAILGUN_DOMAIN_NAME,
            auth=("api", MAILGUN_API_KEY),
            data={"from": "Example Name <example@test.com>",
                "to": successEmailSendTo,
                "subject": successEmailSubjectLine,
                "html": successEmailHtmlMessage},)


    else:

        print('Generating Failed To Execute Notification Email...')

        failedEmailSubjectLine = clientName + ' - KORE Integration - Tickets Unsuccessful for ' + todayDate
        failedEmailSendTo = ''

        failedEmailHtmlMessage = """
            <!DOCTYPE html PUBLIC '-//W3C//DTD XHTML 1.0 Transitional//EN' 'http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd'><html xmlns='http://www.w3.org/1999/xhtml'><head>
            <meta http-equiv='Content-Type' content='text/html; charset=utf-8'>
            <!--[if !mso]><!-->
            <meta http-equiv='X-UA-Compatible' content='IE=edge'>
            <!--<![endif]-->
            <meta name='viewport' content='width=device-width, initial-scale=1.0'>
            <title></title>
            <style type='text/css'>

            </style>
            <!--[if (gte mso 9)|(IE)]>
            <style type='text/css'>
            table {border-collapse: collapse !important;}
            </style>
            <![endif]-->
            <style type='text/css'>

            body {
            margin: 0 !important;
            padding: 0;
            background-color: #ffffff;
            }
            table {
            border-spacing: 0;
            font-family: Arial, Helvetica, sans-serif;
            color: #000000;
            }
            td {
            padding: 0;
            }
            img {
            border: 0;
            }
            div[style*='margin: 16px 0'] { 
            margin:0 !important;
            }
            .wrapper {
            width: 100%;
            table-layout: fixed;
            -webkit-text-size-adjust: 100%;
            -ms-text-size-adjust: 100%;
            }
            .webkit {
            max-width: 600px;
            margin: 0 auto;
            }
            .outer {
            margin: 0 auto;
            width: 100%;
            max-width: 600px;
            }
            .inner {
            padding: 10px;
            }
            .contents {
            width: 100%;
            }
            p, span {
            margin: 0;
            }
            a {
            color: #000000;
            text-decoration: underline;
            }

            .one-column .contents {
            text-align: left;
            }
            .one-column p {
            font-size: 14px;
            margin-bottom: 10px;
            }

            </style>
            </head>
            <body style='padding-top:0;padding-bottom:0;padding-right:0;padding-left:0;margin-top:0 !important;margin-bottom:0 !important;margin-right:0 !important;margin-left:0 !important;'>
            <center class='wrapper' style='width:100%;table-layout:fixed;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;'>
            <div class='webkit' style='max-width:600px;margin-top:0;margin-bottom:0;margin-right:auto;margin-left:auto;'>
              <!--[if (gte mso 9)|(IE)]>
            <table height='100%' width='100%' cellpadding='0' cellspacing='0' border='0'>
            <tr>
              <td valign='top' align='left' background=''>
              <table width='600' align='center' style='border-spacing:0;font-family:Arial, Helvetica, sans-serif;color:#000000;' >
              <tr>
              <td style='padding-top:0;padding-bottom:0;padding-right:0;padding-left:0;' >
              <![endif]-->
              <table bgcolor='#ffffff' cellpadding='0' cellspacing='0' border='0' class='outer' align='center' style='border-spacing:0;font-family:Arial, Helvetica, sans-serif;color:#000000;margin-top:0;margin-bottom:0;margin-right:auto;margin-left:auto;width:100%;max-width:600px;'>
                <tbody>
                  <tr>
                    <td class='one-column' style='padding-top:0;padding-bottom:0;padding-right:0;padding-left:0'>
                      <table cellpadding='0' cellspacing='0' border='0' width='100%'>
                        <tbody><tr>
                          <td class='inner contents' style='padding-left:30px;padding-right:30px;padding-top:10px;padding-bottom:10px;font-family:Avenir TT Book, Arial, Helvetica, sans-serif;font-size:16px;color:#000000;width:100%;text-align:left;'>
                            
                            <p>
                            This is to inform the recipient(s) that the """ + cilientName + """ - KORE Integration - Tickets Process has exceeded the 
                            max number of retries and failed to execute for """ + todayDate + """.
                            </p>
                          </td>
                        </tr>
                      </tbody></table>
                    </td>
                  </tr>
                </tbody>
              </table>
              <!--[if (gte mso 9)|(IE)]>
              </td>
              </tr>
              </table>
            <![endif]-->
            </div>
            </center>

            </body>
            </html>"""

        failedEmailPlainMessage = ''

        print(failedEmailSubjectLine)
        print('Sending Failed To Execute Email...')

        MAILGUN_API_KEY = ''
        MAILGUN_DOMAIN_NAME = ''

        requests.post(
            MAILGUN_DOMAIN_NAME,
            auth=("api", MAILGUN_API_KEY),
            data={"from": "Example Name <example@test.com>",
                "to": failedEmailSendTo,
                "subject": failedEmailSubjectLine,
                "html": failedEmailHtmlMessage},)


if __name__ == '__main__':

    clientName = ''
    successFlag = startTicketIntegration(clientName)
    print('Result of Tickets Integration : {}'.format(successFlag))

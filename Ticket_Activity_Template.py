###
# Title:    ticket_activity_template.py
# Author:   Patrick Galapon
# Date:     2022-08-16
# Version:  1.4
#
# A template for the Kore API Integration into Eloqua, 
# Exports and parses data from the S3 directory set up by Kore and imports into a Ticket Activity CDO in Eloqua.
# To be used for the basis of future Kore API Integation implementations of the Ticket Activity entity.
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
        syncrequest = requests.get(base_url+uri, headers=headers)
        if syncrequest.json()['status'] == 'success':
            return True
        elif syncrequest.json()['status'] == 'pending':
            print('Sync Pending')
            sync(uri, response, base_url)
        elif syncrequest.json()['status'] == 'active':
            print('Sync Churning')
            sync(uri, response, base_url)
        elif syncrequest.json()['status'] == 'warning' or 'error':
            print((requests.get(base_url+uri+'/logs', headers=headers)).json())
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

    # Assign client name 
    clietName = ''

    bucketInfo = {}
    bucketInfo['accessKey'] = AWS_ACCESS_KEY_ID
    bucketInfo['secretAccessKey'] = AWS_SECRET_ACCESS_KEY
    bucketInfo['bucketName'] = bucket_name
    bucketInfo['prefix'] = prefix

    return bucketInfo



def getTicketActivityIntegrationSummary():                

        print('Retrieving Ticket Activity CDO Data Created Today...')

        cdoData = json.dumps({
            "name" : "Retrieve SYSTEM - KORE Ticket Activity CDO Data Created Today",
            "fields" : {
                "pk" : "{{CustomObject[<customDataObjectId>].Field[<fieldId>]}}",
                "emailAddress" : "{{CustomObject[<customDataObjectId>].Field[<fieldId>]}}",
                "createDate" : "{{CustomObject[<customDataObjectId>].CreatedAt}}",
                "updateDate" : "{{CustomObject[<customDataObjectId>].UpdatedAt}}"
            },
            "filter" : "'{{CustomObject[<customDataObjectId>].CreatedAt}}' >= '" + todayDate + "'"
        })

        cdoUrl = bulkUrl + '/customObjects/<customDataObjectId>/exports'
        cdoReqExport = requests.post(cdoUrl, headers=headers, data=cdoData)

        cdoExportUri = cdoReqExport.json()["uri"]

        # Setup sync instance 
        cdoReqExportSyncData = json.dumps({
            "syncedInstanceUri" : cdoExportUri}
        )

        # Sync the data retrieval
        cdoReqSync = requests.post(bulkUrl + '/syncs', headers=headers, data=cdoReqExportSyncData)

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

        cdoReqData = requests.get(bulkUrl + cdoReqSyncInstanceUri + '/data', params='limit=50000', headers=headers)
        #parsedResponse = r.json()
        cdoReqDataCount = int(cdoReqData.json()['count'])

        print('Retrieving first 50000 CDO records...')
        ticketActivityDataCreated = []

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
            ticketActivityDataCreated.append(elem)

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
                ticketActivityDataCreated.append(elem)
            
            cdoHasMore = cdoReqData.json()["hasMore"]
          
        print('Number of Ticket Activity Data Created : {}'.format(len(ticketActivityDataCreated)))

        print('Retrieving Ticket Activity CDO Data Updated Today...')

        cdoData = json.dumps({
            "name" : "Retrieve SYSTEM - KORE Ticket Activity CDO Data Updated Today",
            "fields" : {
                "pk" : "{{CustomObject[<customDataObjectId>].Field[<fieldId>]}}",
                "emailAddress" : "{{CustomObject[<customDataObjectId>].Field[<fieldId>]}}",
                "createDate" : "{{CustomObject[<customDataObjectId>].CreatedAt}}",
                "updateDate" : "{{CustomObject[<customDataObjectId>].UpdatedAt}}"
            },
            "filter" : "'{{CustomObject[<customDataObjectId>].UpdatedAt}}' >= '" + todayDate + "' AND '{{CustomObject[<customDataObjectId>].CreatedAt}}' < '" + todayDate + "'"
        })

        cdoUrl = bulkUrl + '/customObjects/<customDataObjectId>/exports'
        cdoReqExport = requests.post(cdoUrl, headers=headers, data=cdoData)

        cdoExportUri = cdoReqExport.json()["uri"]

        # Setup sync instance 
        cdoReqExportSyncData = json.dumps({
            "syncedInstanceUri" : cdoExportUri}
        )

        # Sync the data retrieval
        cdoReqSync = requests.post(bulkUrl + '/syncs', headers=headers, data=cdoReqExportSyncData)

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

        cdoReqData = requests.get(bulkUrl + cdoReqSyncInstanceUri + '/data', params='limit=50000', headers=headers)
        cdoReqDataCount = int(cdoReqData.json()['count'])

        print('Retrieving first 50000 CDO records...')
        ticketActivityDataUpdated = []

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
            ticketActivityDataUpdated.append(elem)

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
                ticketActivityDataUpdated.append(elem)
            
            cdoHasMore = cdoReqData.json()["hasMore"]
          
        print('Number of Ticket Activity Data Updated : {}'.format(len(ticketActivityDataUpdated)))

        ticketActivityIntegrationSummary = {}
        ticketActivityIntegrationSummary['dataCreated'] = len(ticketActivityDataCreated)
        ticketActivityIntegrationSummary['dataUpdated'] = len(ticketActivityDataUpdated)

        return ticketActivityIntegrationSummary



def startTicketActivityIntegration(clientName):

    apiAccess = getEloquaAccess()
    sitename = apiAccess['site']
    headers = apiAccess['headers']
    baseUrl = apiAccess['baseUrl']

    successFlag = True
    numOfRetries = 1
    actualRetries = 1

    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')

    print('\n')
    print('Started running {} - Ticket_Activity.py : {}'.format(clientName, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

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

            print('Processing KORE files for the ' + clientName)

            # Connect to the bucket
            conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
                            AWS_SECRET_ACCESS_KEY)
            bucket = conn.get_bucket(bucket_name, validate=False)
            # go through the list of files
            bucket_list = bucket.list(prefix=prefix)

            keyStringArray = []
            for l in bucket_list:
                if 'ticketactivity' in str(l.key):
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
                        elem['acctId'] = contentSplit[1]
                        elem['activityName'] = contentSplit[2]
                        elem['addDatetime'] = contentSplit[3]
                        elem['addUser'] = contentSplit[4]
                        elem['assocAcctId'] = contentSplit[5]
                        elem['assocCustNameId'] = contentSplit[6]
                        elem['buyerEmailAddr'] = contentSplit[7]
                        elem['eventDate'] = contentSplit[8]
                        elem['eventId'] = contentSplit[9]
                        elem['eventName'] = contentSplit[10]
                        elem['eventTime'] = contentSplit[11]
                        elem['exportDatetime'] = contentSplit[12]
                        elem['forwardToEmailAddr'] = contentSplit[13]
                        elem['inetTransactionAmount'] = contentSplit[14]
                        elem['koreUpdated'] = contentSplit[15]
                        elem['lastSeat'] = contentSplit[16]
                        elem['name'] = contentSplit[17]
                        elem['numSeats'] = contentSplit[18]
                        elem['orderLineItem'] = contentSplit[19]
                        elem['orderLineItemSeq'] = contentSplit[20]
                        elem['orderNum'] = contentSplit[21]
                        elem['origPurchasePrice'] = contentSplit[22]
                        elem['planEventName'] = contentSplit[23]
                        elem['rowName'] = contentSplit[24]
                        elem['seatNum'] = contentSplit[25]
                        elem['sectionName'] = contentSplit[26]
                        elem['sellerEmailAddr'] = contentSplit[27]
                        elem['tePostingPrice'] = contentSplit[28]
                        elem['tePurchasePrice'] = contentSplit[29]
                        elem['teSellerFees'] = contentSplit[30]
                        elem['teamname'] = contentSplit[31]
                        elem['tmEventName'] = contentSplit[32]
                        elem['tmRowName'] = contentSplit[33]
                        elem['tmSectionName'] = contentSplit[34]
                        elem['seqId'] = contentSplit[35]
                        elem['sortSeq'] = contentSplit[36].replace('\n','')
                    except:
                        continue

                    if elem['buyerEmailAddr'].lower() in emailAddressSet:
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
                            "acct_id" : j['acctId'],
                            "activity_name" : j['activityName'],
                            "add_datetime" : j['addDatetime'],
                            "add_user" : j['addUser'],
                            "assoc_acct_id" : j['assocAcctId'],
                            "assoc_cust_name_id" : j['assocCustNameId'],
                            "buyer_email_addr" : j['buyerEmailAddr'],
                            "event_date" : j['eventDate'],
                            "event_id" : j['eventId'],
                            "event_name" : j['eventName'],
                            "event_time" : j['eventTime'],
                            "export_datetime" : j['exportDatetime'],
                            "forard_to_email_addr" : j['forwardToEmailAddr'],
                            "inet_transaction_amount" : j['inetTransactionAmount'],
                            "kore_updated" : j['koreUpdated'],
                            "last_seat" : j['lastSeat'],
                            "name1" : j['name'],
                            "num_seats" : j['numSeats'],
                            "order_line_item" : j['orderLineItem'],
                            "order_line_item_seq" : j['orderLineItemSeq'],
                            "order_num" : j['orderNum'],
                            "orig_purchase_price" : j['origPurchasePrice'],
                            "plan_event_name" : j['planEventName'],
                            "row_name" : j['rowName'],
                            "seat_num" : j['seatNum'],
                            "section_name" : j['sectionName'],
                            "seller_email_addr" : j['sellerEmailAddr'],
                            "te_posting_price" : j['tePostingPrice'],
                            "te_purchase_price" : j['tePurchasePrice'],
                            "te_seller_fees" : j['teSellerFees'],
                            "teamname" : j['teamname'],
                            "tm_event_name" : j['tmEventName'],
                            "tm_row_name" : j['tmRowName'],
                            "tm_section_name" : j['tmSectionName'],
                            "seq_id" : j['seqId'],
                            "sort_seq" : j['sortSeq']
                            }

                        importContents.append(tmp_contents)

                    print('Import records into CDO {} of {}...'.format(importCounter, len(content_array)))
                    print('Number of records to import: {}'.format(len(importContents)))

                    data = {
                        "name": "SYSTEM - KORE Tickets CDO Import",
                        "fields": {
                            "pk" : "{{CustomObject[14].Field[197]}}",
                            "acct_id" : "{{CustomObject[14].Field[198]}}",
                            "activity_name" : "{{CustomObject[14].Field[199]}}",
                            "add_datetime" : "{{CustomObject[14].Field[200]}}",
                            "add_user" : "{{CustomObject[14].Field[201]}}",
                            "assoc_acct_id" : "{{CustomObject[14].Field[202]}}",
                            "assoc_cust_name_id" : "{{CustomObject[14].Field[203]}}",
                            "buyer_email_addr" : "{{CustomObject[14].Field[204]}}",
                            "event_date" : "{{CustomObject[14].Field[205]}}",
                            "event_id" : "{{CustomObject[14].Field[232]}}",
                            "event_name" : "{{CustomObject[14].Field[233]}}",
                            "event_time" : "{{CustomObject[14].Field[206]}}",
                            "export_datetime" : "{{CustomObject[14].Field[207]}}",
                            "forward_to_email_addr" : "{{CustomObject[14].Field[208]}}",
                            "inet_transaction_amount" : "{{CustomObject[14].Field[209]}}",
                            "kore_updated" : "{{CustomObject[14].Field[210]}}",
                            "last_seat" : "{{CustomObject[14].Field[211]}}",
                            "name1" : "{{CustomObject[14].Field[212]}}",
                            "num_seats" : "{{CustomObject[14].Field[213]}}",
                            "order_line_item" : "{{CustomObject[14].Field[214]}}",
                            "order_line_item_seq" : "{{CustomObject[14].Field[215]}}",
                            "order_num" : "{{CustomObject[14].Field[216]}}",
                            "orig_purchase_price" : "{{CustomObject[14].Field[217]}}",
                            "plan_event_name" : "{{CustomObject[14].Field[218]}}",
                            "row_name" : "{{CustomObject[14].Field[219]}}",
                            "seat_num" : "{{CustomObject[14].Field[220]}}",
                            "section_name" : "{{CustomObject[14].Field[221]}}",
                            "seller_email_addr" : "{{CustomObject[14].Field[222]}}",
                            "te_posting_price" : "{{CustomObject[14].Field[223]}}",
                            "te_purchase_price" : "{{CustomObject[14].Field[224]}}",
                            "te_seller_fees" : "{{CustomObject[14].Field[225]}}",
                            "teamname" : "{{CustomObject[14].Field[226]}}",
                            "tm_event_name" : "{{CustomObject[14].Field[227]}}",
                            "tm_row_name" : "{{CustomObject[14].Field[228]}}",
                            "tm_section_name" : "{{CustomObject[14].Field[229]}}",
                            "seq_id" : "{{CustomObject[14].Field[230]}}",
                            "sort_seq" : "{{CustomObject[14].Field[231]}}",
                            },
                        "identifierFieldName" : "pk",
                        "isSyncTriggeredOnImport" : "true",
                        "mapDataCards" : "true",
                        "mapDataCardsEntityType" : "Contact",
                        "mapDataCardsSourceField": "buyer_email_addr",
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

            ticketActivityIntegrationSummary = getTicketActivityIntegrationSummary()
            ticketActivityDataCreated = ticketActivityIntegrationSummary['dataCreated']
            ticketActivityDataUpdated = ticketActivityIntegrationSummary['dataUpdated']
            writeToTicketSummaryTable(successFlag, clientName, ticketActivityDataCreated, ticketActivityDataUpdated)

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
            insert into <clientName>_Ticket_Activity_Summary values (
                convert(datetime, '{}', 120), 'Success', {}, {}, 0
            )
            """.format(todayDatetime, len(dataCreated), len(dataUpdated))
        c.execute(sqlStmt)

    else:
        sqlStmt = """
            insert into <clientName>_Ticket_Activity_Summary values (
                convert(datetime, '{}', 120), 'Failed', 0, 0, 0
            )
            """.format(todayDatetime)

        #print sqlStmt
        c.execute(sqlStmt)

    db.commit()

    c.close()
    db.close()

    sendNotificationEmal(sFlag, clientName, dataCreated, dataUpdated)



def sendNotificationEmal(sFlag, clientName, dataCreated, dataUpdated):

    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')

    if sFlag:

        print('Generating Successful Notification Email...')

        successEmailSubjectLine = clientName + ' - KORE Integration - Ticket Activity Successful for ' + todayDate
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
                            This is to inform the recipient(s) that the """ + clientName + """ - KORE Integration for Ticket Activity has successfully executed for """ + todayDate + """.
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
                            This is to inform the recipient(s) that the """ + cilientName + """ - KORE Integration - Ticket Activity Process has exceeded the 
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
    successFlag = startTicketActivityIntegration(clientName)
    print('Result of Ticket Activity Integration : {}'.format(successFlag))

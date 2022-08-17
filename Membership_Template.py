###
# Title:    membership_template.py
# Author:   Patrick Galapon
# Date:     2022-08-16
# Version:  1.4
#
# A template for the Kore API Integration into Eloqua, 
# Exports and parses data from the S3 directory set up by Kore and imports into a Membership CDO in Eloqua.
# To be used for the basis of future Kore API Integation implementations of the Membership entity.
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



def getMembershipIntegrationSummary():                

        print('Retrieving Membership CDO Data Created Today...')

        cdoData = json.dumps({
            "name" : "Retrieve SYSTEM - KORE Membership CDO Data Created Today",
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
        membershipDataCreated = []

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
            membershipDataCreated.append(elem)

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
                membershipDataCreated.append(elem)
            
            cdoHasMore = cdoReqData.json()["hasMore"]
          
        print('Number of Membership Data Created : {}'.format(len(membershipDataCreated)))

        print('Retrieving Membership CDO Data Updated Today...')

        cdoData = json.dumps({
            "name" : "Retrieve SYSTEM - KORE Membership CDO Data Updated Today",
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
        membershipDataUpdated = []

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
            membershipDataUpdated.append(elem)

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
                membershipDataUpdated.append(elem)
            
            cdoHasMore = cdoReqData.json()["hasMore"]
          
        print('Number of Membership Data Updated : {}'.format(len(membershipDataUpdated)))

        membershipIntegrationSummary = {}
        membershipIntegrationSummary['dataCreated'] = len(membershipDataCreated)
        membershipIntegrationSummary['dataUpdated'] = len(membershipDataUpdated)

        return membershipIntegrationSummary



def startMembershipIntegration(clientName):

    apiAccess = getEloquaAccess()
    sitename = apiAccess['site']
    headers = apiAccess['headers']
    baseUrl = apiAccess['baseUrl']

    successFlag = True
    numOfRetries = 1
    actualRetries = 1

    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')

    print('\n')
    print('Started running {} - Membership.py : {}'.format(clientName, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

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
                if 'membership' in str(l.key):
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
                        elem['email'] = contentSplit[1]
                        elem['ticketGroupCategory'] = contentSplit[2]
                        elem['ticketGroupName'] = contentSplit[3]
                        elem['ticketGroupDisplayName'] = contentSplit[4]
                        elem['season'] = contentSplit[5]
                        elem['ticketingSystem'] = contentSplit[6].replace('\n','')
                    except:
                        continue

                    if elem['email'].lower() in emailAddressSet:
                        exportData.append(elem)

                print('Number of records to import : {}'.format(len(exportData)))

                #
                # Start of Delete non-existing PKs from Membership CDO
                #

                print 'Start of deleting non-existant PKs from Membership CDO...'

                url = restUrl + '/data/customObject/<customDataObjectId>/instances'
                r = requests.get(url, headers=headers)
                totalElem = r.json()['total']

                arrayId = []
                c = 0
                page = 1
                while c < totalElem:
                    r = requests.get(url + '?page=' + str(page), headers=headers)
                    D = r.json()['elements']

                    for i in D:
                        for j in i['fieldValues']:
                            if j['id'] == '<CDO fieldId of pk in Membership CDO>':
                                elem = {}
                                elem['id'] = i['id']
                                elem['pk'] = j['value']
                                arrayId.append(elem) 

                    c = len(arrayId)
                    page = page + 1

                print 'Number of IDs : %d' % len(arrayId)

                # quit()

                # Create set of current existing PKs
                existingPK = []
                for p in exportData:
                    existingPK.append(p['pk'])
                existingPKSet = set(existingPK)

                arrayIdToDelete = []
                for i in arrayId:
                    if i['pk'] not in existingPKSet:
                        arrayIdToDelete.append(i['id'])

                print 'Number of IDs to delete : %d' % len(arrayIdToDelete)

                # ########## DELETE
                # url = restUrl + '/data/customObject/<customDataObjectId>/instance/'
                # for x in arrayIdToDelete:
                #     r = requests.delete(url + str(x), headers=headers)
                # ########## DELETE

                #
                # End of Delete non-existing PKs from Membership CDO
                #

                fileNum = fileNum + 1
                content_array = [exportData[x:x+10000] for x in xrange(0, len(exportData), 10000)]
                importCounter = 1

                print('Length of content_array : {}'.format(len(content_array)))

                for i in content_array:

                    importContents = []
                    for j in i:
                        tmp_contents = {
                            "email" : i['email'],
                            "ticket_group_category" : i['ticketGroupCategory'],
                            "ticket_group_name" : i['ticketGroupName'],
                            "ticket_group_display_name" : i['ticketGroupDisplayName'],
                            "season" : i['season'],
                            "ticketing_system" : i['ticketingSystem']
                            }

                        importContents.append(tmp_contents)

                    print('Import records into CDO {} of {}...'.format(importCounter, len(content_array)))
                    print('Number of records to import: {}'.format(len(importContents)))

                    data = {
                        "name": "SYSTEM - KORE Membership CDO Import",
                        "fields": {
                            "pk" : "{{CustomObject[13].Field[190]}}",
                            "email" : "{{CustomObject[13].Field[191]}}",
                            "ticket_group_category" : "{{CustomObject[13].Field[192]}}",
                            "ticket_group_name" : "{{CustomObject[13].Field[193]}}",
                            "ticket_group_display_name" : "{{CustomObject[13].Field[194]}}",
                            "season" : "{{CustomObject[13].Field[195]}}",
                            "ticketing_system" : "{{CustomObject[13].Field[196]}}"
                            },
                        "identifierFieldName" : "pk",
                        "isSyncTriggeredOnImport" : "true",
                        "mapDataCards" : "true",
                        "mapDataCardsEntityType" : "Contact",
                        "mapDataCardsSourceField": "email",
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

            successFlag = True

            membershipIntegrationSummary = getMembershipIntegrationSummary()
            membershipDataCreated = membershipIntegrationSummary['dataCreated']
            membershipDataUpdated = membershipIntegrationSummary['dataUpdated']
            writeToMembershipSummaryTable(successFlag, clientName, membershipCreated, membershipUpdated)

            numOfRetries = 7

        except Exception as e:

            print(e)
            print('Failed to run.')

            numOfRetries += 1
            actualRetries += 1

            successFlag = False            
            writeToMembershipSummaryTable(successFlag, clientName, '0', '0')

            time.sleep(300)

    return successFlag



def writeToMembershipSummaryTable(sFlag, clientName, dataCreated, dataUpdated)

    db = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',server='',database='', uid='', pwd='')

    # Set up database cursor
    c = db.cursor()

    todayDatetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if sFlag:
        sqlStmt = """
            insert into <clientName>_Membership_Summary values (
                convert(datetime, '{}', 120), 'Success', {}, {}, 0
            )
            """.format(todayDatetime, len(dataCreated), len(dataUpdated))
        c.execute(sqlStmt)

    else:
        sqlStmt = """
            insert into <clientName>_Membership_Summary values (
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
                            This is to inform the recipient(s) that the """ + clientName + """ - KORE Integration for Membership has successfully executed for """ + todayDate + """.
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
    successFlag = startMembershipIntegration(clientName)
    print('Result of Tickets Integration : {}'.format(successFlag))

import csv
import io
import os
import logging
import sys

ffrCsvPathName = '/home/fracking/data/FracFocusRegistry/CSVs'
ffrCsvFileNameMain = 'RegistryUpload.csv'
ffrCsvFileNamePurpose = 'RegistryUploadPurpose.csv'
ffrCsvFileNameIngredients = 'RegistryUploadIngredients.csv'

ffrUploadDict = {}
ffrPurposeDict = {}
ffrPurposeUploadDict = {}
ffrIngredientsDict = {}

ffrCsvMainFilePath = os.path.join(ffrCsvPathName, ffrCsvFileNameMain)
ffrCsvPurposeFilePath = os.path.join(ffrCsvPathName, ffrCsvFileNamePurpose)
ffrCsvIngredientsFilePath = os.path.join(ffrCsvPathName, ffrCsvFileNameIngredients)

# process the RegistryUpload.csv file
prvFileName = os.path.join(ffrCsvPathName, '%s_prvKeyed.csv' % os.path.splitext(os.path.basename(ffrCsvMainFilePath))[0])
tgtFileName = os.path.join(ffrCsvPathName, '%s_intKeyed.csv' % os.path.splitext(os.path.basename(ffrCsvMainFilePath))[0])
srcFieldnames = ['pKey','JobStartDate','JobEndDate','APINumber','StateNumber','CountyNumber','OperatorName','WellName','Latitude','Longitude','Projection','TVD','TotalBaseWaterVolume','TotalBaseNonWaterVolume','StateName','CountyName','FFVersion','FederalWell']
tgtFieldnames = srcFieldnames.copy()

maxRows = 0
flushRows = 10000

if os.path.exists(prvFileName):
    print ('Importing previous keys from RegistryUpload CSV file: "%s"', prvFileName)
    print ('Please wait...')
    print ('')
    rows = 0
    with io.open(prvFileName, 'r', encoding='windows-1252', newline='') as csvSource:
        dictReader = csv.DictReader(csvSource, fieldnames=['pKey','pInt'], delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        for row in dictReader:
            rows += 1
            if rows > 1:
                pKey = row['pKey'].strip()
                ffrUploadDict[pKey] = row['pInt']
            if flushRows > 0 and rows % flushRows == 0:
                print ('Rows processed: %d' % rows)
    print ('%s: Rows processed: %d' % (tgtFileName, rows))
    print ('')

print ('Converting RegistryUpload rows from CSV file: "%s"', prvFileName)
print ('Please wait...')
print ('')
with io.open(tgtFileName, 'w', encoding='windows-1252') as csvTarget:
    dictWriter = csv.DictWriter(csvTarget, fieldnames=tgtFieldnames, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    dictWriter.writeheader()
    with io.open(ffrCsvMainFilePath, 'r', encoding='windows-1252', newline='') as csvSource:
        dictReader = csv.DictReader(csvSource, fieldnames=srcFieldnames, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        rows = 0
        thisKey = 0
        for row in dictReader:
            if rows > 0:
                pKey = row['pKey'].strip()
                try:
                    pKey = ffrUploadDict[pKey]
                except KeyError:
                    thisKey += 1
                    ffrUploadDict[pKey] = thisKey
                    pKey = thisKey
                row['pKey'] = pKey
                row['JobStartDate'] = row['JobStartDate'].strip()[:10]
                row['JobEndDate'] = row['JobEndDate'].strip()[:10]
                dictWriter.writerow(row)
            rows += 1
            if flushRows > 0 and rows % flushRows == 0:
                print ('Rows processed: %d' % rows)
            if maxRows > 0 and rows >= maxRows:
                break
    print ('%s: Rows processed: %d' % (tgtFileName, rows))
    print ('')

# output dictionary for later use the next time this program is executed
print ('Outputting previous keys to CSV file: "%s"', prvFileName)
print ('Please wait...')
print ('')
rows = 0
with io.open(prvFileName, 'w', encoding='windows-1252') as csvTarget:
    dictWriter = csv.DictWriter(csvTarget, fieldnames=['pKey','pInt'], delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    dictWriter.writeheader()
    rows += 1
    for key, value in ffrUploadDict.items():
        rows += 1
        dictWriter.writerow({'pKey': key, 'pInt': value})
        if flushRows > 0 and rows % flushRows == 0:
            print ('Rows processed: %d' % rows)
    print ('%s: Rows processed: %d' % (prvFileName, rows))
    print ('')

# process the RegistryUploadPurpose.csv file

prvFileName = os.path.join(ffrCsvPathName, '%s_prvKeyed.csv' % os.path.splitext(os.path.basename(ffrCsvPurposeFilePath))[0])
tgtFileName = os.path.join(ffrCsvPathName, '%s_intKeyed.csv' % os.path.splitext(os.path.basename(ffrCsvPurposeFilePath))[0])

if os.path.exists(prvFileName):
    print ('Importing previous keys from RegistryPurpose CSV file: "%s"', prvFileName)
    print ('Please wait...')
    print ('')
    rows = 0
    with io.open(prvFileName, 'r', encoding='windows-1252', newline='') as csvSource:
        dictReader = csv.DictReader(csvSource, fieldnames=['pKey','pInt'], delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        for row in dictReader:
            rows += 1
            if rows > 1:
                pKey = row['pKey'].strip()
                ffrPurposeDict[pKey] = row['pInt']
            if flushRows > 0 and rows % flushRows == 0:
                print ('Rows processed: %d' % rows)
    print ('%s: Rows processed: %d' % (tgtFileName, rows))
    print ('')

print ('Converting RegistryPurpose rows from CSV file: "%s"', prvFileName)
print ('Please wait...')
print ('')
with io.open(tgtFileName, 'w', encoding='windows-1252') as csvTarget:
    fieldnamesSource = ['pKey','pKeyRegistryUpload','TradeName','Supplier','Purpose']
    fieldnamesTarget = ['pKey','pKeyUpload','TradeName','Supplier','Purpose']
    dictWriter = csv.DictWriter(csvTarget, fieldnames=fieldnamesTarget, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    dictWriter.writeheader()
    with io.open(ffrCsvPurposeFilePath, 'r', encoding='windows-1252', newline='') as csvSource:
        dictReader = csv.DictReader(csvSource, fieldnames=fieldnamesSource, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        rows = 0
        thisKey = 0
        for row in dictReader:
            if rows > 0:
                pKey = row['pKey'].strip()
                pKeyUpload = row['pKeyRegistryUpload'].strip()
                try:
                    ffrPurposeUploadDict[pKey] = ffrUploadDict[pKeyUpload]
                except KeyError:
                    ffrPurposeUploadDict[pKey] = None
                try:
                    pKey = ffrPurposeDict[pKey]
                except KeyError:
                    thisKey += 1
                    ffrPurposeDict[pKey] = thisKey
                    pKey = thisKey
                row['pKey'] = pKey
                row['pKeyUpload'] = ffrUploadDict[pKeyUpload]
                del row['pKeyRegistryUpload']
                dictWriter.writerow(row)
            rows += 1
            if flushRows > 0 and rows % flushRows == 0:
                print ('Rows processed: %d' % rows)
            if maxRows > 0 and rows >= maxRows:
                break
    print ('%s: Rows processed: %d' % (tgtFileName, rows))
    print ('')

# output dictionary for later use the next time this program is executed
print ('Outputting previous keys to CSV file: "%s"', prvFileName)
print ('Please wait...')
print ('')
rows = 0
with io.open(prvFileName, 'w', encoding='windows-1252') as csvTarget:
    dictWriter = csv.DictWriter(csvTarget, fieldnames=['pKey','pInt'], delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    dictWriter.writeheader()
    rows += 1
    for key, value in ffrPurposeDict.items():
        rows += 1
        dictWriter.writerow({'pKey': key, 'pInt': value})
        if flushRows > 0 and rows % flushRows == 0:
            print ('Rows processed: %d' % rows)
    print ('%s: Rows processed: %d' % (prvFileName, rows))
    print ('')

# process the RegistryUploadIngredients.csv file
prvFileName = os.path.join(ffrCsvPathName, '%s_prvKeyed.csv' % os.path.splitext(os.path.basename(ffrCsvIngredientsFilePath))[0])
tgtFileName = os.path.join(ffrCsvPathName, '%s_intKeyed.csv' % os.path.splitext(os.path.basename(ffrCsvIngredientsFilePath))[0])

if os.path.exists(prvFileName):
    print ('Importing previous keys from RegistryIngredient CSV file: "%s"', prvFileName)
    print ('Please wait...')
    print ('')
    rows = 0
    with io.open(prvFileName, 'r', encoding='windows-1252', newline='') as csvSource:
        dictReader = csv.DictReader(csvSource, fieldnames=['pKey','pInt'], delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        for row in dictReader:
            rows += 1
            if rows > 1:
                ffrIngredientsDict[row['pKey']] = row['pInt']
            if flushRows > 0 and rows % flushRows == 0:
                print ('Rows processed: %d' % rows)
    print ('%s: Rows processed: %d' % (tgtFileName, rows))
    print ('')

print ('Converting RegistryIngredient rows from CSV file: "%s"', prvFileName)
print ('Please wait...')
print ('')
with io.open(tgtFileName, 'w', encoding='windows-1252') as csvTarget:
    fieldnamesSource = ['pKey','pKeyPurpose','IngredientName','CASNumber','PercentHighAdditive','PercentHFJob','IngredientComment','IngredientMSDS','MassIngredient']
    fieldnamesTarget = ['pKey','pKeyPurpose','pKeyUpload','IngredientName','CASNumber','PercentHighAdditive','PercentHFJob','IngredientComment','IngredientMSDS','MassIngredient']
    dictWriter = csv.DictWriter(csvTarget, fieldnames=fieldnamesTarget, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    dictWriter.writeheader()
    with io.open(ffrCsvIngredientsFilePath, 'r', encoding='windows-1252', newline='') as csvSource:
        dictReader = csv.DictReader(csvSource, fieldnames=fieldnamesSource, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        rows = 0
        thisKey = 0
        for row in dictReader:
            if rows > 0:
                pKey = row['pKey'].strip()
                pKeyPurpose = row['pKeyPurpose'].strip()
                try:
                    pKey = ffrIngredientsDict[pKey]
                except KeyError:
                    thisKey += 1
                    ffrIngredientsDict[pKey] = thisKey
                    pKey = thisKey
                row['pKey'] = pKey
                row['pKeyPurpose'] = ffrPurposeDict[pKeyPurpose]
                row['pKeyUpload'] = ffrPurposeUploadDict[pKeyPurpose]
                dictWriter.writerow(row)
            rows += 1
            if flushRows > 0 and rows % flushRows == 0:
                print ('Rows processed: %d' % rows)
            if maxRows > 0 and rows >= maxRows:
                break
    print ('%s: Rows processed: %d' % (tgtFileName, rows))
    print ('')

# output dictionary for later use the next time this program is executed
print ('Outputting previous keys to CSV file: "%s"', prvFileName)
print ('Please wait...')
print ('')
rows = 0
with io.open(prvFileName, 'w', encoding='windows-1252') as csvTarget:
    dictWriter = csv.DictWriter(csvTarget, fieldnames=['pKey','pInt'], delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    dictWriter.writeheader()
    rows += 1
    for key, value in ffrIngredientsDict.items():
        rows += 1
        dictWriter.writerow({'pKey': key, 'pInt': value})
        if flushRows > 0 and rows % flushRows == 0:
            print ('Rows processed: %d' % rows)
    print ('%s: Rows processed: %d' % (prvFileName, rows))
    print ('')

print ('====================')
print ('Processing finished!')
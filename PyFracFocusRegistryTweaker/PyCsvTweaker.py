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

tgtFileName = os.path.join(ffrCsvPathName, '%s_intKeyed.csv' % os.path.splitext(os.path.basename(ffrCsvMainFilePath))[0])

with io.open(tgtFileName, 'w', encoding='windows-1252') as csvTarget:
    fieldnames = ['pKey','JobStartDate','JobEndDate','APINumber','StateNumber','CountyNumber','OperatorName','WellName','Latitude','Longitude','Projection','TVD','TotalBaseWaterVolume','TotalBaseNonWaterVolume','StateName','CountyName','FFVersion','FederalWell']
    dictWriter = csv.DictWriter(csvTarget, fieldnames=fieldnames, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    dictWriter.writeheader()
    with io.open(ffrCsvMainFilePath, 'r', encoding='windows-1252', newline='') as csvSource:
        dictReader = csv.DictReader(csvSource, fieldnames=fieldnames, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        rows = 0
        maxRows = 0
        flushRows = 10000
        thisKey = 0
        for row in dictReader:
            if rows > 0:
                try:
                    pKey = ffrUploadDict[row['pKey'].strip()]
                except KeyError:
                    thisKey += 1
                    ffrUploadDict[row['pKey'].strip()] = thisKey
                    pKey = ffrUploadDict[row['pKey'].strip()]
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

# process the RegistryUploadPurpose.csv file

tgtFileName = os.path.join(ffrCsvPathName, '%s_intKeyed.csv' % os.path.splitext(os.path.basename(ffrCsvPurposeFilePath))[0])

with io.open(tgtFileName, 'w', encoding='windows-1252') as csvTarget:
    fieldnamesSource = ['pKey','pKeyRegistryUpload','TradeName','Supplier','Purpose']
    fieldnamesTarget = ['pKey','pKeyUpload','TradeName','Supplier','Purpose']
    dictWriter = csv.DictWriter(csvTarget, fieldnames=fieldnamesTarget, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    dictWriter.writeheader()
    with io.open(ffrCsvPurposeFilePath, 'r', encoding='windows-1252', newline='') as csvSource:
        dictReader = csv.DictReader(csvSource, fieldnames=fieldnamesSource, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        rows = 0
        maxRows = 0
        flushRows = 10000
        thisKey = 0
        for row in dictReader:
            if rows > 0:
                pKey = row['pKey'].strip()
                pKeyUpload = row['pKeyRegistryUpload'].strip()
                try:
                    pKey = ffrPurposeDict[pKey]
                except KeyError:
                    thisKey += 1
                    ffrPurposeDict[pKey] = thisKey
                    ffrPurposeUploadDict[pKey] = ffrUploadDict[pKeyUpload]
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

# process the RegistryUploadIngredients.csv file

tgtFileName = os.path.join(ffrCsvPathName, '%s_intKeyed.csv' % os.path.splitext(os.path.basename(ffrCsvIngredientsFilePath))[0])

with io.open(tgtFileName, 'w', encoding='windows-1252') as csvTarget:
    fieldnamesSource = ['pKey','pKeyPurpose','IngredientName','CASNumber','PercentHighAdditive','PercentHFJob','IngredientComment','IngredientMSDS','MassIngredient']
    fieldnamesTarget = ['pKey','pKeyPurpose','pKeyUpload','IngredientName','CASNumber','PercentHighAdditive','PercentHFJob','IngredientComment','IngredientMSDS','MassIngredient']
    dictWriter = csv.DictWriter(csvTarget, fieldnames=fieldnamesTarget, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    dictWriter.writeheader()
    with io.open(ffrCsvIngredientsFilePath, 'r', encoding='windows-1252', newline='') as csvSource:
        dictReader = csv.DictReader(csvSource, fieldnames=fieldnamesSource, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        rows = 0
        maxRows = 0
        flushRows = 10000
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

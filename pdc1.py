################################################################################
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#
# This file and all of it's contents are CONFIDENTIAL and the sole property of
# Groove Health. Disclosing any logic, names, or data whatsoever found within
# this file will result in legal dispute. If you are not an employee at Groove
# Health and recieved this file in error, immediately delete it from all sources
# you are aware of.
#
# Thank you,
# The Groove Health Software Development Team
#
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
################################################################################

################################################################################
#
# John Thiesen
# Groove Health
# March 6, 2018
#
# pdc0.py - 2nd attempt at doing pdc calculations for Medicare star measure
#           This one is supposed to count the number of ndcs for the patient
################################################################################

import datetime
from datetime import timedelta
import monthdelta
from dateutil import parser

today = datetime.date(2015, 1, 1)
sixMonthsAgo = datetime.date(2013, 1, 1)
starMeasureType = '1'


import pyodbc
import random
import string
import gc
import csv


from groovedb2 import ODBC_CONN_STR

startTime = datetime.datetime.now()




comma = ","


dbConn = pyodbc.connect( ODBC_CONN_STR, timeout=100 )
dbCurs = dbConn.cursor()
dbConn2 = pyodbc.connect( ODBC_CONN_STR, timeout=100 )
dbCurs2 = dbConn2.cursor()


pdcFile = open('pdc_detail.csv', 'w')
#pdcFile.write("Pdc_row_id,Patient_id,Star_measure_type,Pdc_value,Date_calculated, Index_date, Index_period, Days_covered, Hosp_ind\n")

def retrievePatients ():
    sql = ("SELECT Patient_id FROM gdw_patient_medicare_view")
    dbCurs.execute(sql)
    recs = dbCurs.fetchall()
    return recs

def hasInsulinClaims(patient):
    sql = "SELECT * FROM gdw_pharm_claim_insulin as clm WHERE clm.Patient_id = " + patient + " AND clm.Filled_date >= '" + sixMonthsAgo.isoformat() + "' AND clm.Filled_date < '" + today.isoformat() + "'"
    dbCurs.execute(sql)
    recs = dbCurs.fetchall()
    return len(recs)
    
def hasESRD(patient):
    sql = ("SELECT * FROM gdw_icd_esrd_view as esrd WHERE esrd.Patient_id = " +
        patient +
        " AND esrd.Admit_date >= '" +
        sixMonthsAgo.isoformat() +
        "' AND esrd.Admit_date < '" +
        today.isoformat() + "'")
    dbCurs.execute(sql)
    recs = dbCurs.fetchall()
    return len(recs)

def readClaims(patient):
    sql = ("SELECT * FROM gdw_pharm_claim_generic as clm WHERE clm.Patient_id = " +
            patient + " AND clm.Filled_date >= '" + sixMonthsAgo.isoformat() + "' AND clm.Filled_date < '" + today.isoformat() + "'")
    dbCurs.execute(sql)
    recs = dbCurs.fetchall()
    return recs

    
def hospAdjust(patient, indexDate, daysChart):
    sql = ("SELECT Admit_date, Discharge_date FROM gdw_claim_inpatient_view as clm WHERE clm.Patient_id = " +
            patient + " AND clm.Admit_date >= '" + indexDate.isoformat() + "' AND clm.Admit_date < '" + today.isoformat() + "'")
    dbCurs.execute(sql)
    recs = dbCurs.fetchall()
    print (patient, " hosptial claims ", len(recs))
    for rec in recs:
        print (patient, " hospital claim ", rec.Admit_date, "   ", rec.Discharge_date)
        dayPointer = parser.parse(rec.Admit_date).date()
        dayDischarge = parser.parse(rec.Discharge_date).date()
        while (dayPointer < today and dayPointer <= dayDischarge):
            daysChart[dayPointer].add("hosp")
            print ("added hosp at ", dayPointer)
            dayPointer = dayPointer + timedelta(days=1)
    return




def basicPDCCalc (clmLst):
    indexDateString = "ZZZZZZZZ"
    for clm in clmLst:
        if (clm[2] < indexDateString):
            indexDateString = clm[2]
        print(str(clm[1]) + '   Filled date:' + clm[2] + '   Days supply:' + str(clm[3]) + '   Generic name:' + clm[5] + '\n')
    indexDate = parser.parse(indexDateString)
    print("index date   ", indexDate)

    daysChart = {}
    dayCounter = indexDate.date()
    while (dayCounter < today): #create a dictionary that is basically a calendar, in order to fill in which days are covered; the key is the date, the value will be a set containing the generic names prescribed for that day; need to turn this into a class sometime
        daysChart[dayCounter] = set()
        dayCounter = dayCounter + timedelta(days=1)
    for clm in clmLst: #now go thru the claims and fill in the calendar, adjusting for overlaps with the same generic name
        supplyStartDate = parser.parse(clm[2]).date()
        supplyEndDate = supplyStartDate + timedelta(days=clm[3])
        while (supplyStartDate < supplyEndDate and supplyStartDate < today):
            if (clm[5] not in daysChart[supplyStartDate]): #if a day is not marked with a particular generic, then mark it and move to the next day
                daysChart[supplyStartDate].add(clm[5])
            else:
                supplyEndDate = supplyEndDate + timedelta(days=1) #if the day is already marked, this means there is an overlap and thus we shift the end date of the prescription by one
            supplyStartDate = supplyStartDate + timedelta(days=1)
    hospAdjust(str(clm[1]), indexDate.date(), daysChart)
    #print (daysChart)
    hospDays = 0
    #OK, NOW we have to go thru the daysChart and simply count the number of days with non-empty sets and then divide by the total number of days since the index date
    #but that doesn't account for inpatient days yet
    dayNumerator = 0
    for day, generic in daysChart.items():
        #print (generic)
        if (len(generic) > 0 and not(len(generic) == 1 and "hosp" in generic)): #don't count if it's ONLY a hospital day with no prescription
            dayNumerator += 1
        if (len(generic) > 1 and "hosp" in generic): #don't count unless it's a hospital day plus a prescription
            hospDays += 1
    print ("hospital days ", hospDays)
    print ("dayNumerator ", dayNumerator)
    print ("days in index period ", (today - indexDate.date()).days)
    dayDenominator = ((today - indexDate.date()).days) - hospDays
    print ("dayDenominator  ", dayDenominator)
    if (dayNumerator > dayDenominator):
        pdcValue = 1 #in this case, the days in the index period have been reduced by the number of hospital days, meaning that the prescription has been pushed past the last day of the index period
    else:
        pdcValue = (dayNumerator / dayDenominator)
    pdcValue = pdcValue * 100
    if (hospDays == 0):
        hospIndicator = 'N'
    else:
        hospIndicator = 'Y'
    ndcCount = countNdcs(clmLst)   
    pdcFile.write('' + comma + str(patient.Patient_id) + comma + starMeasureType + comma + str(round(pdcValue,2)) + comma + str(today) + comma + str(indexDate.date()) + comma + str(dayDenominator) + comma + str(dayNumerator) + comma + hospIndicator +  comma + str(ndcCount) + comma + '' + '\n')
    return pdcValue

def countNdcs(clmLst):
    ndcCount = 0
    ndcSet = set()
    for clm in clmLst:
        ndcSet.add(clm[4])
    ndcCount = len(ndcSet)
    return ndcCount
    
#######################################
#######################################
########################################
#here's the main loop
########################################

patientList = retrievePatients()
print('patient count   ', len(patientList), '\n')

for patient in patientList:
    insulinCount = hasInsulinClaims(str(patient.Patient_id)) #eliminate patients who've had insulin claims
    #print(patient.Patient_id, '   insulin count  ', insulinCount, '\n')
    if (insulinCount > 0):
        continue
    ESRDCount = hasESRD(str(patient.Patient_id)) #eliminate patients with ESRD claims
    #print (patient.Patient_id, '   ESRD count  ', ESRDCount, '\n')
    if (ESRDCount > 0):
        continue
    claimList = readClaims(str(patient.Patient_id))
    #print(patient.Patient_id, '   claim count  ', len(claimList), '\n')
    if (len(claimList) == 0):
        continue
    if (len(claimList) == 1):
        print ("Projected eligible " + patient.Patient_id)
        continue
    pdcValue = basicPDCCalc(claimList)
    print(patient.Patient_id, '   pdc:  ', pdcValue, '*************\n')

pdcFile.close()













        
print()
print( "COMPLETED IN "+str(datetime.datetime.now() - startTime) )

                                 
dbConn.close()
dbConn2.close()




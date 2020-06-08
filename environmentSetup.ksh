#!/usr/bin/ksh
#################################################################################
# Zahir Sheikh
#
# v0.1 - 05-29-2020 - Created  
#################################################################################
# This framework accept two parameters (i.e. Source Schema and Target Schema ) in order 
# to sync up the database schema by calling the python script, which internally connecting
# Stage DB with Hardcoded connection parameters.
# This script is divided into eight logical section i.e.
# 1. Exporting Oracle variables to setup the bash environment
# 2. Assignment to local variables as well as Log file by name ${logDir}/envSetup_from*.log 
# 3. Failure function terminate execution of this shell script and sends an email to Support 
#	 Team with attach log.
# 4. Create Backup folder in TARGET directory and transfer jobs & trans directories into it. 
#	 Copy above folders from SOURCE to TARGET folder.
# 5. Create Backup folder in temp TARGET directory and transfer all files into it. After this 
#	 copy all the temp files from source to target directory.
# 6. Create Metadata directory for current sync up process and execute python framework.
# 7. Purging all the 60/30 days old backup directory and files from TARGET, TEMP_FILES & METADATA directories respectively.
# 8. Send the Success email to Support Team once Job is finished
#################################################################################

export ORACLE_HOME=/export/third-party/oracle/product/11.2.0/client_1
export LD_LIBRARY_PATH=$ORACLE_HOME/lib
export ORACLE_SID=TARGET

#################################################################################
# Set some internal variables and create metaData directoty
progNameLong=`basename $0`                                      # name of the program, minus all of the path info
progNameShort=`echo ${progNameLong} | sed 's/\.[^.]*$//'`       # name of the program, minus the extension
dateTime=`date +%m%d%Y%H%M%S`                                   # datetime stamp mmddyyyyhhmiss
date=`date +%m%d%Y`                                   			# date stamp mmddyyyy
currPID=$$                              						# PID of the current running process (knowing this helps to kill the process)

# assignment to local variables.
sourceSchema=$1
targetSchema=$2
rootDir=/paas/projects								  	# root direcotry path
targetDir=${rootDir}/${targetSchema}
sourceDir=${rootDir}/${sourceSchema}
targetBkpDir=${targetDir}/backUP_${dateTime}
metaDataPath=${rootDir}/envSetup/metaData/${sourceSchema}_${targetSchema}_${dateTime}	  # meta data folder name 

logDir=/paas/log/envSetup                                       # the log working directory is constant
logFile=${logDir}/envSetup_from_${sourceSchema}_to_${targetSchema}_${dateTime}_${currPID}.log  #temporary log   


tempFilesPath=/paas/temp_files
tempTargetDir=${tempFilesPath}/${targetSchema}
tempSourceDir=${tempFilesPath}/${sourceSchema}
tempTargetBkpDir=${tempTargetDir}/backUP_${date}

#################################################################################
failureFunction ()
{
echo "IN FAILURE BLOCK " >> ${logFile}
#mailx -s "enviornmentSetup Job Failed" -a "${logFile}" "zahiriqubal.sheikh@cognizant.com" <<< "PFA Failure log for your reference !"
exit 0212
}

#################################################################################
mkdir ${targetBkpDir}
status=$?
if [[ $status -ne 0 ]]; then

    echo "*******ERROR******" >> ${logFile}
    echo "TARGET BACKUP DIRECOTY CREATION IS FAILED WITH EXIT CODE: ${status}" >> ${logFile}
    failureFunction
else
    echo "TARGET BACKUP DIRECOTY IS CREATED SUCCESSFULLY !!!!! : ${targetBkpDir}" >> ${logFile}
	mv ${targetDir}/jobs ${targetBkpDir}
	status=$?
	if [[ $status -ne 0 ]]; then

		echo "*******ERROR******" >> ${logFile}
		echo "TARGET JOBS FOLDER IS MOVED TO BACKUP DIRECOTY FAILED WITH EXIT CODE: ${status}" >> ${logFile}
		failureFunction
	else
		echo "TARGET JOBS FOLDER IS MOVED TO BACKUP DIRECOTY SUCCESSFULLY !!!!! : ${targetBkpDir}" >> ${logFile}
		cp -r ${sourceDir}/jobs ${targetDir}	
		status=$?
		if [[ $status -ne 0 ]]; then

			echo "*******ERROR******" >> ${logFile}
			echo "SOURCE JOBS FOLDER COPY TO TARGET DIRECOTY IS FAILED WITH EXIT CODE: ${status}" >> ${logFile}
			failureFunction
		else
			echo "SOURCE JOBS FOLDER IS COPIED TO TARGET DIRECOTY SUCCESSFULLY !!!!! : ${targetDir}/jobs" >> ${logFile}
		fi
	fi
	mv ${targetDir}/trans ${targetBkpDir}
	status=$?
	if [[ $status -ne 0 ]]; then

		echo "*******ERROR******" >> ${logFile}
		echo "TARGET TRANS FOLDER IS MOVED TO BACKUP DIRECOTY FAILED WITH EXIT CODE: ${status}" >> ${logFile}
		failureFunction
	else
		echo "TARGET TRANS FOLDER IS MOVED TO BACKUP DIRECOTY SUCCESSFULLY !!!!! : ${targetBkpDir}" >> ${logFile}
		cp -r ${sourceDir}/trans ${targetDir}	
		status=$?
		if [[ $status -ne 0 ]]; then

			echo "*******ERROR******" >> ${logFile}
			echo "SOURCE TRANS FOLDER COPY IS FAILED WITH EXIT CODE: ${status}" >> ${logFile}
			failureFunction
		else
			echo "SOURCE TRANS FOLDER IS COPIED TO TARGET DIRECOTY SUCCESSFULLY !!!!! : ${targetDir}/trans" >> ${logFile}
		fi
	fi	
fi

#################################################################################
mkdir ${tempTargetBkpDir}
status=$?
if [[ $status -ne 0 ]]; then

    echo "*******ERROR******" >> ${logFile}
    echo "TEMP_FILES BACKUP DIRECTORY CREATION IS FAILED WITH EXIT CODE: ${status}" >> ${logFile}
    failureFunction
else
	echo "MOVING ALL FILES OF TARGET TEMP_FILES TO BACKUP DIRECTORY : ${tempTargetBkpDir}" >> ${logFile}
	find ${tempTargetDir} -maxdepth 1 -type f -print0 | xargs -0 mv -t ${tempTargetBkpDir}
	if [ ${?} -ne 0 ]
	then
		echo "*******ERROR******" >> ${logFile}
		echo "TEMP_FILES TRANSFER TO BACKUP DIRECTORY IS FAILED WITH EXIT CODE: ${status}" >> ${logFile}
		failureFunction
	fi
	find ${tempSourceDir} -maxdepth 1 -type f -print0 | xargs -0 cp -t ${tempTargetDir}
	if [ ${?} -ne 0 ]
	then
		echo "*******ERROR******" >> ${logFile}
		echo "TEMP_FILES TRANSFER FROM SOURCE TO TRAGET DIRECTORY IS FAILED WITH EXIT CODE: ${status}" >> ${logFile}
		failureFunction
	fi

fi

#################################################################################
mkdir $metaDataPath														  # create metadata folder to store the audit files
status=$?
if [[ $status -ne 0 ]]; then
	echo "*******ERROR******" >> ${logFile}
	echo "TEMP_FILES TRANSFER TO BACKUP DIRECTORY IS FAILED WITH EXIT CODE: ${status}" >> ${logFile}
	failureFunction
else
	#################################################################################
	# Start the logical processing by executing python script and takes care of Table, Index, Function, Procedure, Grantor and Grantee sync up
	# python /paas/projects/envSetup/envionmentSetup.py 'MIGRATION_8' 'MIGRATION_6' '/paas/projects/envSetup/metaData'
	cd /paas/projects/envSetup
 	./environmentSetup.py $sourceSchema $targetSchema $metaDataPath >> ${logFile}
	status=$? 											#Exit with the exit code of the python script
	if [[ $status -ne 0 ]]; then
		echo "*******ERROR******" >> ${logFile}
		echo "PYTHON SCRIPT FAILED WITH EXIT CODE: ${status}" >> ${logFile}
		failureFunction
	fi
fi

#################################################################################
# clean up old files from the log archive directory
echo "Begin - Removing TARGET BACKUP DIRECTORY and files from ${targetDir}/backUp* older than 60 days" >> ${logFile}
#find ${targetDir} -mindepth 1 -maxdepth 2 -type d -mtime +60 -print0 | xargs -0 rm -rf
find ${targetDir} -mindepth 1 -maxdepth 2 -type d -mtime +60 -exec rm -rf {} \;
if [ ${?} -ne 0 ]
then
	echo "*******ERROR******" >> ${logFile}
	echo "60 DAYS PURGE OF TARGET BACKUP DIRECTORY FAILED WITH EXIT CODE: ${status}" >> ${logFile}
	failureFunction
fi
echo "Completed - Removing TARGET BACKUP DIRECTORY and files from ${targetDir}/backUp* older than 60 days" >> ${logFile}

echo "Begin - Removing TEMP BACKUP DIRECTORY and files from ${tempTargetDir}/backUp* older than 60 days" >> ${logFile}
#find ${tempTargetDir} -mindepth 1 -maxdepth 2 -type d -mtime +60 -print0 | xargs -0 rm -rf
find ${tempTargetDir} -mindepth 1 -maxdepth 2 -type d -mtime +60 -exec rm -rf {} \;
if [ ${?} -ne 0 ]
then
	echo "*******ERROR******" >> ${logFile}
	echo "60 DAYS PURGE OF TEMP BACKUP DIRECTORY FAILED WITH EXIT CODE: ${status}" >> ${logFile}
	failureFunction
fi
echo "Completed - Removing TARGET BACKUP DIRECTORY and files from ${tempTargetDir}/backUp* older than 60 days" >> ${logFile}

echo "Begin - Removing METADATA DIRECTORIES and files from ${rootDir}/envSetup/metaData* older than 30 days" >> ${logFile}
#find ${rootDir}/envSetup/metaData -mindepth 1 -maxdepth 2 -type d -mtime +30 -print0 | xargs -0 rm -rf
find ${rootDir}/envSetup/metaData -mindepth 1 -maxdepth 2 -type d -mtime +30 -exec rm -rf {} \;
if [ ${?} -ne 0 ]
then
	echo "*******ERROR******" >> ${logFile}
	echo "30 DAYS PURGE OF METADATA DIRECTORIES FAILED WITH EXIT CODE: ${status}" >> ${logFile}
	failureFunction
fi
echo "Completed - Removing METADATA DIRECTORIES and files from ${rootDir}/envSetup/metaData* older than 30 days" >> ${logFile}
#################################################################################
#mailx -s "enviornmentSetup Job Finished" "zahiriqubal.sheikh@cognizant.com" <<< "Congratulation ${sourceSchema} ${targetSchema} Enviornment Setup Completed  !"
exit 0

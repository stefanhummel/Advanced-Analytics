#!/bin/bash
###############################################################
# Licensed Materials - Property of IBM
# © Copyright IBM Corp. 2016 All Rights Reserved.
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
###############################################################
VERSION="dashDB spark-submit.sh v1.1.1"
#/dashdb-api/analytics/public/apps/submit
#-----------------------Endpoints-Start----------------------------#
SS_CURL_OPTIONS=" --insecure --silent --user "
PUBLIC_ENDPNT="/dashdb-api/analytics/public"
LOAD_ENDPNT=$PUBLIC_ENDPNT"/samples/load"
SUBMIT_ENDPNT=$PUBLIC_ENDPNT"/apps/submit" 
CANCELJOB_ENDPNT=$PUBLIC_ENDPNT"/apps/cancel?submissionid="
STATUS_ENDPNT=$PUBLIC_ENDPNT"/monitoring/cluster_status"
APPSTATUS_ENDPNT=$PUBLIC_ENDPNT"/monitoring/app_status"
SUBSTATUS_ENDPNT=$APPSTATUS_ENDPNT"?submissionid="
DEALLOC_ENDPNT=$PUBLIC_ENDPNT"/cluster/deallocate"
APPS_ENDPNT="/dashdb-api/home/spark/apps"
DEFAULT_LIBS_ENDPNT="/dashdb-api/home/spark/defaultlibs"
GLOBAL_LIBS_ENDPNT="/dashdb-api/global"
LOGS_FOLDER_ENDPNT="/dashdb-api/home/spark/log"
LOGS_ENDPNT=$LOGS_FOLDER_ENDPNT"/latest"
USERS_ENDPNT="/dashdb-api/users"
#-----------------------Endpoints-End----------------------------#

#-----------------------PrintOutputs-Start----------------------------#

printHelp(){
    typ=$1
    exitCode=$2
    case $typ in
    usage)
        printf "\nUse the spark-submit.sh script to launch or manage Apache Spark applications within dashDB."
        printf "\n  Usage:"
        printf "\n    spark-submit.sh <jar_filename> [args] --class <class_name> [app-options] [--jsonout]"
        printf "\n    spark-submit.sh <py_filename> [args] [app-options] [--jsonout]"
        printf "\n    spark-submit.sh [other-option] [--user <user_name>] [--jsonout]"
        printf "\n    spark-submit.sh --help"
        ;;
    all)
        printf "\nUse the spark-submit.sh script to launch or manage Apache Spark applications within dashDB."
        printf "\n  Usage:"
        printf "\n    spark-submit.sh <jar_filename> [args] --class <class_name> [app-options] [--jsonout]"
        printf "\n    spark-submit.sh <py_filename> [args] [app-options] [--jsonout]"
        printf "\n    spark-submit.sh [other-option] [--user <user_name>] [--jsonout]"
        printf "\n    spark-submit.sh --help"
        printf "\n"
        printf "\n  Parameters:"
        printf "\n    <jar_filename>"
        printf "\n       The name of the file that contains the Java or Scala application code that is to be submitted."
        printf "\n       This file must be in the \$home/spark/apps directory on the dashDB host system."
        printf "\n    <py_filename>"
        printf "\n       The name of the file that contains the Python application code that is to be submitted. This"
        printf "\n       file must be in the \$home/spark/apps directory on the dashDB host system."
        printf "\n    [args]"
        printf "\n       Arguments that are required as input for the application that is being run."
        printf "\n    <class_name>"
        printf "\n       The main class of a Java or Scala application."
        printf "\n    [app-options]"
        printf "\n       One or more of the following options:"
        printf "\n          --jars <files>"
        printf "\n              For application code that is written in Java or Scala, a comma-separated"
        printf "\n              list of any .jar files that are used by the application."
        printf "\n          --py-files <files> "
        printf "\n              For application code that is written in Python, a comma-separated list of"
        printf "\n              any .zip, .egg, or .py files that are used by the application."
        printf "\n          --name <name>"
        printf "\n              The name that is to be assigned to your application. If not specified, the"
        printf "\n              name is set to the class name (Java/Scala application) or file name (Python"
        printf "\n              application)."
        printf "\n          --master <url>"
        printf "\n              If the dashDB URL to which the application is to be submitted is different from"
        printf "\n              the dashDB URL that is currently set, use this option to specify the new URL."
        printf "\n"    
        printf "\n    [other-option]"   
        printf "\n       One of the following options, each of which corresponds to a command:"
        printf "\n          --load-samples"                
        printf "\n              Load files containing sample Spark application code into your"
        printf "\n              \$home/spark/apps directory on the dashDB host system."
        printf "\n          --cluster-status"
        printf "\n              Retrieve information about the status of your Spark cluster or, if you have"
        printf "\n              administrator privileges, the clusters of all users."
        printf "\n          --app-status <submission_id>"
        printf "\n              Retrieve information about the status of the application with the specified "
        printf "\n              submission ID."
        printf "\n          --list-apps"
        printf "\n              Retrieve information about all applications that are currently running or that "
        printf "\n              ran since the cluster was last started."
        printf "\n          --upload-file [apps|defaultlibs|globallibs] <file> "
        printf "\n              Upload the specified file to a directory on the host:"
        printf "\n                 apps         The \$home/spark/apps directory on the dashDB host system, which"
        printf "\n                              contains files that are to be available to a particular application."
        printf "\n                              This is the default."
        printf "\n                 defaultlibs  The \$home/spark/defaultlibs directory on the dashDB host system, which"
        printf "\n                              contains files that are to be available to all of your applications."
        printf "\n                 globallibs   The /globallibs directory on the dashDB host system, which contains"
        printf "\n                              files that are to be available to all applications for all users."
        printf "\n                              (For administrators only.)"
        printf "\n              An administrator can issue this command on behalf of another user by specifying the"
        printf "\n              --user option after the file name."
        printf "\n          --download-file [apps|defaultlibs|globallibs] <file> [--dir <target_path>]"
        printf "\n              Download the specified file to the current directory from the source directory on the "
        printf "\n              host:"
        printf "\n                 apps         The \$home/spark/apps directory on the dashDB host system, which"
        printf "\n                              contains files that are to be available to a particular application."
        printf "\n                              This is the default."
        printf "\n                 defaultlibs  The \$home/spark/defaultlibs directory on the dashDB host system, which"
        printf "\n                              contains files that are to be available to all of your applications."
        printf "\n                 globallibs   The /globallibs directory on the dashDB host system, which contains"
        printf "\n                              files that are to be available to all applications for all users."
        printf "\n                              (For administrators only.)"
        printf "\n              An administrator can issue this command on behalf of another user by specifying the"
        printf "\n              --user option after the file name."
        printf "\n          --delete-file [apps|defaultlibs|globallibs] <file>  "
        printf "\n              Delete the specified file from a directory on the host:"
        printf "\n                 apps         The \$home/spark/apps directory on the dashDB host system, which"
        printf "\n                              contains files that are to be available to a particular application."
        printf "\n                              This is the default."
        printf "\n                 defaultlibs  The \$home/spark/defaultlibs directory on the dashDB host system, which"
        printf "\n                              contains files that are to be available to all of your applications."
        printf "\n                 globallibs   The /globallibs directory on the dashDB host system, which contains"
        printf "\n                              files that are to be available to all applications for all users."
        printf "\n                              (For administrators only.)"
        printf "\n              An administrator can issue this command on behalf of another user by specifying the"
        printf "\n              --user option after the file name."
        printf "\n          --list-files [apps|defaultlibs|globallibs]  "
        printf "\n              List all the files that are in a directory on the host:"
        printf "\n                 apps         The \$home/spark/apps directory on the dashDB host system, which"
        printf "\n                              contains files that are to be available to a particular application."
        printf "\n                              This is the default."
        printf "\n                 defaultlibs  The \$home/spark/defaultlibs directory on the dashDB host system, which"
        printf "\n                              contains files that are to be available to all of your applications."
        printf "\n                 globallibs   The /globallibs directory on the dashDB host system, which contains"
        printf "\n                              files that are to be available to all applications for all users."
        printf "\n                              (For administrators only.)"
        printf "\n              An administrator can issue this command on behalf of another user by specifying the"
        printf "\n              --user option."
        printf "\n          --display-app-log <app|out|err|info> [submission_id] "
        printf "\n              Display, for the application with the specified submission ID, the contents of the"
        printf "\n              log file of the indicated type (application, standard output, standard error, or"
        printf "\n              information). If no submission ID is specified, the log file of the most recently "
        printf "\n              submitted application is displayed."
        printf "\n          --download-app-logs [submission_id] [--dir <target_path>]"
        printf "\n              Download the standard error, standard output, information, and application log files " 
        printf "\n              of the application with the specified submission ID. If no submission ID is specified, "
        printf "\n              the log files of the most recently submitted application are downloaded."
        printf "\n          --display-cluster-log <err|out> <master|worker> [worker-host-ip]"
        printf "\n              Display, standard error/output logs of the dashDB spark cluster master and worker process."
        printf "\n          --download-cluster-logs [--dir <target_path>]"
        printf "\n              Download the standard error and standard output logs of master and worker process of"
        printf "\n              dashDB spark cluster."
        printf "\n          --kill <submission_id> "
        printf "\n              Cancel the running application with the specified submission ID."
        printf "\n          --webui-url "
        printf "\n              Retrieve the URL of the Spark Web User Interface (UI)."
        printf "\n          --env  "
        printf "\n              Retrieve the settings of the environment variables used by the spark-submit.sh script."
        printf "\n          --version "
        printf "\n              Display the version of the spark-submit.sh script that you are using."
        printf "\n          --help "
        printf "\n              Display this help message."
        printf "\n"
        printf "\n    --jsonout "
        printf "\n              Display the output of a command in JSON format. This overrides the value specified by"
        printf "\n              the DASHDBJSONOUT environment variable. This does not apply to the --display-log, "
        printf "\n              --version, --env, --webui-url, or --help commands."
        printf "\n    --user <user_name>"
        printf "\n              Execute the command on behalf of another user. This option can be used only by an "
        printf "\n              administrator, and applies only to the --upload-file, --download-file, --upload-file," 
        printf "\n              and --list-file commands."
        printf "\n    --dir <target_path>"
        printf "\n              Specify the target directory to download the files. This option can be used only with"
        printf "\n              --download-file, --download-app-logs and --download-cluster-logs."
        #printf "\n      --deallocate                    Deallocates the spark cluster allocated by the user."
        
        if [ -z ${DASHDBURL} ] && [ -z ${DASHDBUSER} ] && [ -z ${DASHDBPASS} ];
        then
            printf "\n"
            printf "\n User can also choose to set the following environment variables in order to prevent entering them everytime interactively."
            printf "\n export DASHDBURL=https://<dashDB-host>:8443"
            printf "\n export DASHDBUSER=<UserName>"
            printf "\n export DASHDBPASS=<Password>"
        fi
        ;;
    esac
    printf "\n"
    exit $exitCode
}

consolePrint(){
    local output_line=$1
    printf "%s\n" "${output_line}"
}

#validate_if_secureconn(){
 #TODO:Discuss and implement an if condition for the variable $SS_CURL_OPTIONS
#}

printResult(){
    result="$1"
    tags=$2
    
    if [[ ( -z $DASHDBJSONOUT ) ||
            ( "$DASHDBJSONOUT" == "NO" ) ]];
    then
        
        for tag in "${tags[@]}"
        do
            tagval=""
            IFS=","
            for field in $result; do
                tagval="$(echo $field | grep -w ${tag})"
                
                if [ ! -z $tagval ];then
                
                    tagval=$(echo $tagval | sed 's/\"//g' | sed 's/{//g' | sed 's/\}//g' | sed 's/\[//g' | sed 's/\]//g')
                    break;
                fi
            done
 
            if [ ! -z $tagval ];
            then
                #echo $tag":"$tagval
                tag="$(echo $tag)"
                case "$tag" in
                    status)
                        tagval=$(echo $tagval | cut -d \: -f2)
                        consolePrint "Status: "$tagval
                        ;;
                    statusDesc)
                        tagval=$(echo $tagval | cut -d \: -f2)
                        consolePrint "Description: "$tagval
                        ;;
                    running_apps)
                        tagval=$(echo $tagval | cut -d \: -f3)
                        consolePrint "Running Jobs: "$tagval
                        ;;
                    submissionId)
                        typ="$3"
                        if [ "$typ" == "submit" ];
                        then
                            tagval=$(echo $tagval | cut -d \: -f2)
                            consolePrint "Submission ID: "$tagval
                        elif [ "$typ" == "status" ];
                        then
                            tagval=$(echo $tagval | cut -d \: -f3)
                            consolePrint "Submission ID: "$tagval
                        fi
                        ;;
                    applicationId)
                        tagval=$(echo $tagval | cut -d \: -f2)
                        consolePrint "Application ID: "$tagval
                        ;;
                    resultCode)
                        typ="$3"
                        path="$4"
                        if [ "$typ" == "upload" ];
                        then
                            tagval=$(echo $tagval | cut -d \: -f2)
                            consolePrint "Status: "$tagval
                            if [ "SUCCESS" == "$tagval" ];
                            then
                                consolePrint "Message: File uploaded successfully to folder '$path'"
                            elif [ "ERROR" == "$tagval" ];
                            then
                                consolePrint "File upload failed."
                            fi
                        elif [ "$typ" == "delete" ];
                        then
                            file=$4
                            tagval=$(echo $tagval | cut -d \: -f2)
                            consolePrint "Status: "$tagval
                            if [ "SUCCESS" == "$tagval" ];
                            then
                                consolePrint "Message: File '$file' deleted successfully from dashDB system."
                            elif [ "ERROR" == "$tagval" ];
                            then
                                file=$4
                                consolePrint "Message: File delete failed. File '$file' does not exist."
                            fi
                        elif [ "$typ" == "download" ];
                        then
                            path=$4
                            tagval=$(echo $tagval | cut -d \: -f2)
                            consolePrint "Status: "$tagval
                            if [ "SUCCESS" == "$tagval" ];
                            then
                                consolePrint "Message: File(s) have been downloaded to the folder '$path'" 
                            elif [ "ERROR" == "$tagval" ];
                            then
                                consolePrint "Message: File download failed. File '$path' does not exist."
                            fi
                        elif [ "$typ" == "" ];
                        then
                            tagval=$(echo $tagval | cut -d \: -f2)
                            consolePrint "resultCode: "$tagval
                        fi
                        ;;
                    clusters)
                        consolePrint "Clusters:"
                        tags=("running_apps" "username")
                        #The Only square bracket present in the string and can be used to extract clusters.
                        clusters="$(echo $result | cut -d \[ -f2 | cut -d \] -f1)"
                        #Each cluster in present within {} brackets
                        IFS="{"
                        for cluster in $clusters; 
                        do
                            if [ "$cluster" != "" ];
                            then
                                user=""
                                appcnt=""
                                #echo $cluster
                                for tag in "${tags[@]}"; 
                                do
                                    val=""
                                    IFS=" " #After first IFS=',', the result string is replaced by spaces
                                    for field in $cluster; do
                                        val="$(echo $field | grep -w ${tag})"
                                        if [ ! -z $val ]; then
                                            val=$(echo $val | sed 's/\"//g' | sed 's/{//g' | sed 's/\}//g' | sed 's/\[//g' | sed 's/\]//g')
                                            break 1;
                                        fi
                                    done
#                                    echo $val   
                                    if [ "$tag" == "username" ];
                                    then
                                        user="$(echo $val | cut -d \: -f2)"
                                    elif [ "$tag" == "running_apps" ];
                                    then
                                        appcnt="$(echo $val | cut -d \: -f2)"
                                    fi
                                done
                                
                                consolePrint " $user: $appcnt Running Applications"
                            fi
                        done
                        ;;
                    message)
                        tagval=$(echo $tagval | cut -d \: -f2)
                        consolePrint "Message: "$tagval
                        ;;
                    code)
                        tagval=$(echo $tagval | cut -d \: -f3)
                        consolePrint "ExitCode: "$tagval
                        ;;
                    apps)
                        printf "\n"
                        printstr=$printstr"SubmissionID;ApplicationID;Status\n"
                        printstr=$printstr"---------------------;------------------------;-------\n"
                        tags=("submissionId" "applicationId" "status")
                        applications=$(echo $result | sed 's/submissionId/\!submissionId/g')
                        IFS="!"
                        for app in $applications;
                        do
                            app=$(echo $app | sed 's/ /\#/g')
                            if [ "$app" != "" ];
                            then
                                subid=""
                                appid=""
                                status=""
                                for tag in "${tags[@]}"; 
                                do
                                    val=""
                                    IFS="#"
                                    for field in $app; do
                                        val="$(echo $field | grep -w ${tag})"
                                        if [ ! -z $val ]; then
                                            val=$(echo $val | sed 's/\"//g' | sed 's/{//g' | sed 's/\}//g' | sed 's/\[//g' | sed 's/\]//g')
                                            val=$(echo $val | sed "s/$tag/\!$tag/g" | cut -d \! -f2 | cut -d \, -f1)
                                            break 1;
                                        fi
                                    done
#                                    echo "Value: "$val
                                    if [ "$val" != "" ];
                                    then
                                        if [ "$tag" == "submissionId" ];
                                        then
                                            subid="$(echo $val | cut -d \: -f2)"
                                        elif [ "$tag" == "applicationId" ];
                                        then
                                            appid="$(echo $val | cut -d \: -f2)"
                                            if [ "$appid" == "" ];
                                            then
                                                appid="                       "
                                            fi
                                        elif [ "$tag" == "status" ];
                                        then
                                            status="$(echo $val | cut -d \: -f2)"
                                        fi
                                    fi
                                done
                                
                                if [ "$subid" != "" ];
                                then
                                    printstr=$printstr"$subid;$appid;$status\n"
                                fi
                            fi
                        done
                        echo -e "${printstr}" | column -s ';' -t -x
                        printf "\n"
                        ;;
                esac
            fi
        done
        
    elif [ "$DASHDBJSONOUT" == "YES" ];
    then
        consolePrint "$result"
    fi
}

printDisclaimer(){
    hasjsonout="FALSE"
    while [[ $# > 0 ]];
    do
        input=$1
        if [ $input == "--jsonout" ];
        then
            hasjsonout="TRUE"
            break 1
        fi
        shift
    done
    
    if [[ ( "$hasjsonout" == "FALSE" ) && ( "$DASHDBJSONOUT" != "YES" ) ]];
    then
        echo "--- dashDB spark-submit.sh ---"
    fi
}

#-----------------------PrintOutputs-End----------------------------#

#-----------------------HandleLogin-Start----------------------------#
validateAdmin(){
    output=$1
    if [ ! -z $output ];
    then
        IFS=","
        for field in $output; do
            val=$(echo "$field" | grep -w "role")
            if [ ! -z $val ];then
                break;
            fi
        done
        #Unset IFS
        IFS=$' \t\n'
        role=$(echo $val | cut -d \: -f3 | cut -d \" -f2)
        
        if [ $role == "Administrator" ];
        then
            export ISADMIN="TRUE"
        else
            export ISADMIN="FALSE"
        fi
    fi
}

validateCredentials(){
    DASHDBURL=${DASHDBURL%/} #Formatting URL:-Removes the last also if present
    loginendpoint=${DASHDBURL}/dashdb-api/users/${DASHDBUSER}
    output=$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -X GET $loginendpoint)

    if [[ "$output" != *'"userid":"'${DASHDBUSER}'"'* ]] ||
          [[ "$output" != *'"resultCode":"SUCCESS"'* ]] ||
             [[ "$output" != *'"errorMessageCode":"NONE"'* ]];
    then
        
        consolePrint "Login failure. dashDB URL or user credentials provided are invalid."
        exit 1;    
    else
        validateAdmin $output
    fi
}

userLogin(){
    if [[ ( ! -z ${DASHDBURL} )  && ( -z ${DASHDBUSER}  || -z ${DASHDBPASS} ) ]];
    then
        consolePrint "Enter your dashDB login credentials for ${DASHDBURL}"
    elif [ ! -z ${DASHDBURL} ] && [ ! -z ${DASHDBUSER} ] && [ -z ${DASHDBPASS} ];
    then        
        consolePrint "Enter your dashDB login credentials for ${DASHDBURL}"
        consolePrint [${DASHDBUSER}]
    elif [ -z ${DASHDBUSER} ] || [ -z ${DASHDBURL} ] || [ -z ${DASHDBPASS} ];
    then
        consolePrint "Enter your dashDB URL and credentials:"
    fi
    
    
    while true; do
        if [ -z ${DASHDBURL} ];
        then
            read -p "dashDB URL (https://<dashDB-host>:8443): " DASHDBURL   
        
        elif [ -z ${DASHDBUSER} ];
        then
            read -p "User name: " DASHDBUSER
        
        elif [ -z ${DASHDBPASS} ];
        then
            echo -n "Password: " 
            read -s DASHDBPASS
            echo ""
        else
            validateCredentials
            break
        fi
    done
}

validateUser(){
    user="$1"
    if [ "$ISADMIN" == "TRUE" ];
    then
        output=$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XGET ${DASHDBURL}${USERS_ENDPNT}/$user)
        val=""
        IFS=","
        for field in $output; do
            val=$(echo "$field" | grep -w "resultCode")
            if [ ! -z $val ];
            then
                break;
            fi
        done
        
        res=$(echo $val |  cut -d \: -f2 | cut -d \" -f2)

        if [ "$res" == "ERROR" ];
        then
            tags=("resultCode" "message")
            printResult "$output" $tags
            exit 1
        elif [ "$res" == "SUCCESS" ];
        then
            export FILEOPUSER=$user
        fi
    elif [ "$ISADMIN" == "FALSE" ];
    then
        consolePrint "You are not authorized to perform this operation."
        exit 1
    fi
}

#-----------------------HandleLogin-End----------------------------#

#-----------------------Environment-Start----------------------------#
setenviron(){
    export LC_ALL=C
}

validatejsonout(){
    if [[ ( ! -z $DASHDBJSONOUT ) && 
            ( "$DASHDBJSONOUT" != "NO" ) &&
            ( "$DASHDBJSONOUT" != "YES" ) ]]; then
        consolePrint "Value of \$DASHDBJSONOUT is incorrectly set. Expected values: YES, NO. Defaults to NO."    
        export DASHDBJSONOUT=NO
    fi
}

validate_environment(){
    if [ -z ${DASHDBURL} ] && [ -z ${DASHDBUSER} ] && [ -z ${DASHDBPASS} ];
    then
        printf "\n"
        consolePrint "User can also choose to set the following environment variables in order to prevent entering them everytime interactively."
        consolePrint "export DASHDBURL=https://<dashDB-host>:8443"
        consolePrint "export DASHDBUSER=<UserName>"
        consolePrint "export DASHDBPASS=<Password>"
        printf "\n"
    fi    
    userLogin
    if [ ! -z $PARMUSER ];
    then
        validateUser $PARMUSER
    fi
    validatejsonout
    return 0
}

display_env_variables(){
    consolePrint "DASHDBURL=${DASHDBURL}"
    consolePrint "DASHDBUSER=${DASHDBUSER}"
    consolePrint "DASHDBPASS=${DASHDBPASS}"
    consolePrint "DASHDBJSONOUT"=${DASHDBJSONOUT}
}
#-----------------------Environment-End----------------------------#

#-------------------Handle App Submission-START--------------------#

handleSubmit(){
    properties=""
    mainclass=""
    submitfile=""
    appArgs=""
    header="Content-Type:application/json;charset=UTF-8"
    
    while [[ $# > 0 ]];
    do
        input=$1
        case $input in
            --master)
                export DASHDBURL=$2
                shift
                shift
                ;;
            --name)
                properties=$properties'"sparkAppName":"'$2'", '
                shift
                shift
                ;;
            --py-files)
                properties=$properties'"sparkSubmitPyFiles":"'$2'", '
                shift
                shift
                ;;
            --jars)
                properties=$properties'"sparkJars":"'$2'", '
                shift
                shift
                ;;
#            --driver-library-path)
#                properties=$properties'"sparkDriverExtraLibraryPath":"'$2'", '
#                shift
#                shift
#                ;;
#            --driver-class-path)
#                properties=$properties'"sparkDriverExtraClassPath":"'$2'", '
#                shift
#                shift
#                ;;
            --class)
                mainclass="${2}"
                shift
                shift
                ;;
            *.jar|*.py)
                #If invalid input and if arguments are set before Main App
                if [[ "${input}" != ^--.* ]] &&  [[ ! -z "${appArgs}" ]]; 
                then 
                    consolePrint "Syntax error. Unrecognized input option: ${input}."
                    printHelp "usage" 1
                else
                    submitfile="${input}"
                fi
                shift
                ;;
            --jsonout)
                export DASHDBJSONOUT=YES
                shift
                ;;
            *)  
                #If invalid input and if Main App file not set. Without App no need of appArgs
                if [[ "${input}" != ^--.* ]] &&  [[ -z "${submitfile}" ]]; 
                then 
                    consolePrint "Syntax error. Unrecognized input option: ${input}."
                    printHelp "usage" 1
                else

#                    if [ -z "${appArgs}" ]
#                    then
                    appArgs=$appArgs'"'${input}'", ' 
#                    fi
                    shift
                fi
                ;;    
        esac
    done
    
    data='{ '
    if [ -z $submitfile ];
    then
        consolePrint "Syntax error. No application file was specified "
        printHelp "usage" 1
    else
        validate_environment
        if [ -f $submitfile ];
        then
#            uploadfile $submitfile
            hdr="Content-Type: multipart/form-data"
            curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XPOST -H "$hdr" -F data=@"$submitfile" ${DASHDBURL}${APPS_ENDPNT} -o /dev/null
            submitfile=$(basename $submitfile)
        fi
        data=$data'"appResource" : "'$submitfile'", '
        
        if [ ! -z $mainclass ];
        then
            data=$data'"mainClass":"'$mainclass'", '
        fi
        
        if [ "$appArgs" != "" ];
        then
            appArgs=$(echo $appArgs | sed 's/.\{1\}$//') #Remove the last space and comma
            data=$data'"appArgs":['$appArgs'], '
        fi
        if [ ! -z "$properties" ];
        then
            properties=$(echo $properties | sed 's/.\{1\}$//')
            #properties="$(echo $properties | sed 's/.\{1\}$//')"#Remove the last space and comma
            data=$data'"sparkProperties" :  {'$properties'}, '
        fi
        #echo $data | sed 's/.\{2\}$//'
        data=$(echo $data | sed 's/.\{1\}$//') #Remove the last space and comma
        data=$data' }'
        
        #validate_environment
        result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XPOST ${DASHDBURL}${SUBMIT_ENDPNT} --header "$header" --data "$data")"
        tags=("status" "statusDesc" "submissionId" "code" "message")
        typ="submit"
        printResult "$result" $tags $typ
    fi
       
}
#-------------------Handle App Submission-END--------------------#
#---------------------FileOperations-START-----------------------#
getfolderendpoint(){
    foldertyp=$1
    file=$2
    endpoint=""
    
    case $foldertyp in
        "apps")
            endpoint=${APPS_ENDPNT}
            ;;
        "defaultlibs")
            endpoint=${DEFAULT_LIBS_ENDPNT}
            ;;
        "globallibs")
            if [ $ISADMIN == "TRUE" ];
            then
                endpoint=${GLOBAL_LIBS_ENDPNT}
                
            elif [ $ISADMIN == "FALSE" ];
            then
                #consolePrint "You are not authorized to access this location."
                return 1
            fi
            ;;
    esac
    
    if [ "$file" != "" ];
    then
        endpoint="$endpoint/$file"
    fi
    
    if [ "$FILEOPUSER" != "" ];
    then
        endpoint="$endpoint?user="$FILEOPUSER
    fi
    
    echo $endpoint
}

checkfileexists(){
    typ=$1
    case $typ in
    local)
        file=$2
        dir=""
        if [ "$DOWNLOAD_TARGET" != "" ];
        then
            DOWNLOAD_TARGET=${DOWNLOAD_TARGET%/} #Formatting Path:-Removes the last '/' if present
            dir=$DOWNLOAD_TARGET
        else
            dir="$(pwd)"
        fi
        
        if [[ -f $dir/$file || -d $dir/$file ]];
        then
            read -p "File with name $file already present. Do you want to overwrite(default-y)?[y/n] : " overwrite
            if [[ "$overwrite" == 'n' || "$overwrite" == 'N' ]];
            then
                consolePrint "Message: Download cancelled."
                return 1
            fi
        fi
        return 0
        ;;
    dashdb)
        #file_endpoint=$2
        folder=$2
        file=$3
        file_endpoint=$(getfolderendpoint $folder $file)
        result_code="$(eval "curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -w %{http_code} -XGET ${DASHDBURL}$file_endpoint -o /dev/null")"
        if [ "$result_code" == "200" ];
        then
            read -p "File with name $file is already present on dashDB system. Do you want to overwrite(default-y)?[y/n] : " overwrite
            if [[ "$overwrite" == 'n' || "$overwrite" == 'N' ]];
            then
                consolePrint "Message: Upload cancelled."
                return 1
            fi
        fi
        return 0
        ;;
    esac
    
}

uploadfile(){
    header="Content-Type: multipart/form-data"
    result=""
    file_loc=""
    endpoint=""
    foldertyp=""
    
    printerror(){
        msg=$1
        consolePrint "$msg Provide appropriate inputs to upload the file."
        consolePrint "    spark-submit.sh --upload-file [apps|defaultlibs|globallibs] <file> [--jsonout] [--user <user_name>]"
        exit 1;
    }
    
    if [ -z $2 ];
    then
        file_loc=$1
        foldertyp="apps"
    elif [[ ( ! -z $2 ) && ( "$1" != "apps" ) && ( "$1" != "defaultlibs" ) && ( "$1" != "globallibs" ) ]];
    then
            msg="Invalid folder location '$1' provided."
            printerror "$msg"
    else
        file_loc=$2
        foldertyp="$1"    
    fi
    
    if [ ! -f $file_loc ];
    then
        msg="Invalid file path provided."
        printerror "$msg"
    fi
    
    validate_environment
    endpoint=$(getfolderendpoint $foldertyp)
    out=$?
    if [ $out -eq 1 ];
    then
        consolePrint "You are not authorized to access this location."
        exit 1
    fi
    
    file="$(basename $file_loc)"
    checkfileexists "dashdb" $foldertyp $file
    exists=$?
    if [ $exists -eq 1 ];
    then
        exit 1
    fi

    result="$(eval "curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XPOST -H '"$header"' -F data=@"$file_loc" ${DASHDBURL}$endpoint")"
    tags=("resultCode") 
    typ="upload"
    dir=$foldertyp
    printResult "$result" $tags $typ $dir
}

downloadfile(){
    result=""
    file=""
    endpoint=""
    foldertyp=""
    printerror(){
        msg=$1
        consolePrint "$msg Provide appropriate inputs to download the file."
        consolePrint "    spark-submit.sh --download-file [apps|defaultlibs|globallibs] <file> [--dir <target_path>] [--jsonout] [--user <user_name>]"
        exit 1;
    }
    
    if [ -z $2 ];
    then
        if [[ ( $1 == "apps" ) || ( $1 == "defaultlibs" ) || ( $1 == "globallibs" ) ]];
        then
            msg="File to download is not provided."
            printerror "$msg"
        else
            file=$1
            foldertyp="apps"
        fi
    elif [[ ( ! -z $2 ) && ( $1 != "apps" ) && ( $1 != "defaultlibs" ) && ( $1 != "globallibs" ) ]];
    then
        msg="Invalid folder location '$1' provided."
        printerror "$msg"
    else
        file="$2"
        foldertyp="$1"
    fi
    
    validate_environment
    endpoint="$(getfolderendpoint $foldertyp $file)"
    out=$?
    if [ $out -eq 1 ];
    then
        consolePrint "You are not authorized to access this location."
        exit 1
    fi
    
    dir=""
    if [ "$DOWNLOAD_TARGET" != "" ];
    then
        dir=$DOWNLOAD_TARGET
    else
        dir="$(pwd)"
    fi
    
    #Check if the file path is valid. If file exists, it gives result code 200 else result code 400.
    checkfileexists "local" $file
    exists=$?
    if [ $exists -eq 1 ];
    then
        exit 1
    fi
    
    result_code="$(eval "curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -w "%{http_code}" -XGET ${DASHDBURL}$endpoint -o $dir/$file")"
    if [ "$result_code" == "200" ];
    then
        result='{"resultCode":"SUCCESS"}' #Construct the SUCCESS result message to provide to the printResult function
        tags=("resultCode")
        printResult $result $tags "download" "$dir"
    elif [ "$result_code" == "400" ];
    then
        error="$(cat $dir/$file)"
        tags=("resultCode")
        file_on_server="$foldertyp/$file"
        printResult "$error" $tags "download" "$file_on_server"
        rm -f $dir/$file
        exit 1
    fi
    
}

deletefile(){
    endpoint=""
    file=""
    endpoint=""
    foldertyp=""
    
    printerror(){
        msg=$1
        consolePrint "$msg Provide appropriate inputs to delete the file."
        consolePrint "    spark-submit.sh --delete-file [apps|defaultlibs|globallibs] <file> [--jsonout] [--user <user_name>]"
        exit 1;
    }
    
    if [ -z $2 ];
    then
        if [[ ( $1 == "apps" ) || ( $1 == "defaultlibs" ) || ( $1 == "globallibs" ) ]];
        then
            msg="File to delete is not provided."
            printerror "$msg"
        else
            foldertyp="apps"
            file=$1
        fi
    elif [[ ( ! -z $2 ) && ( $1 != "apps" ) && ( $1 != "defaultlibs" ) && ( $1 != "globallibs" ) ]];
    then
        msg="Invalid folder location '$1' provided."
        printerror "$msg"
    else
        foldertyp="$1"
        file="$2"    
    fi
    
    validate_environment
    endpoint="$(getfolderendpoint $foldertyp $file)"
    out=$?
    if [ $out -eq 1 ];
    then
        consolePrint "You are not authorized to access this location."
        exit 1
    fi
    
    result="$(eval "curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -X DELETE ${DASHDBURL}$endpoint")"
    typ="delete"
    tags=("resultCode")
    file_on_server="$foldertyp/$file"
    printResult "$result" $tags $typ $file_on_server
}


listfiles(){
    folderTyp=$1
    endpoint=""
    
    printerror(){
        loc=$1
        consolePrint "Invalid folder location '$loc' provided. Provide appropriate inputs to list files."
        consolePrint "    spark-submit.sh --list-files [apps|defaultlibs|globallibs] [--jsonout] [--user <user_name>]"
        exit 1;
    }
    
    if [ -z $folderTyp ];
    then
        folderTyp="apps"
    fi    
    
    if [[ ( "$folderTyp" != "apps" && "$folderTyp" != "defaultlibs" && "$folderTyp" != "globallibs" ) ]];
    then
        printerror $folderTyp
    fi
    
    validate_environment
    endpoint="$(getfolderendpoint $folderTyp)"
    out=$?
    if [ $out -eq 1 ];
    then
        consolePrint "You are not authorized to access this location."
        exit 1
    elif [ "" == "$endpoint" ];
    then
        printerror $folderTyp
    fi

    result="$(eval "curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -X GET ${DASHDBURL}$endpoint")"
    val=""
    IFS=","
    for field in $result; do
      val=$(echo "$field" | grep -w "result")
      if [ ! -z $val ];
      then
          break;
      fi
    done
    #Unset IFS
    
    IFS=$' \t\n'
    list=$(echo $val | cut -d \: -f2 | cut -d \" -f2)
    
    if [ -z $list ];
    then
        consolePrint "User has not uploaded any files. Use --upload-file option to upload files or --load-samples to load example files."
        exit 0;
    elif [ $list == '[]' ];
    then
        consolePrint "Folder does not exist on the dashDB system."
    else
        if [ "$DASHDBJSONOUT" == "YES" ];
        then
            consolePrint "$result"
        else
            echo -e $list | sed -e 's/^\(\.\\\/\)/\ /'
        fi
    fi
}

#---------------------FileOperations-END-----------------------#
#-----------------------OtherOperations-START-------------------------#
getSubIds(){
    result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -X GET ${DASHDBURL}${LOGS_FOLDER_ENDPNT})"
    IFS=","
    for field in $result; do
      val=$(echo "$field" | grep -w "result")
      if [ ! -z $val ];then
            break;
      fi
    done
     
    list=$(echo $val | cut -d \: -f2)
    subIds="$(echo -e $list | grep submission_ | cut -d \_ -f2 | grep -v [a-zA-z])"
    echo -e $subIds
}

getClusterId(){
    result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -X GET ${DASHDBURL}${LOGS_FOLDER_ENDPNT})"
    IFS=","
    for field in $result; do
      val=$(echo "$field" | grep -w "result")
      if [ ! -z $val ];then
            break;
      fi
    done
     
    list=$(echo $val | cut -d \: -f2)
    clustId="$(echo -e $list | grep cluster_ | cut -d \_ -f2 | grep -v [a-zA-z])"
    echo -e $clustId
}

list_submittedapps(){
    result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -X GET ${DASHDBURL}${APPSTATUS_ENDPNT})"
    
    if [ "$DASHDBJSONOUT" == "YES" ];
    then
        consolePrint "$result"
    else
        apps="$(echo $result | cut -d \[ -f2 | cut -d \] -f1)"
        if [ "$apps" == "" ];
        then
            consolePrint "User has not submitted any application. Spark application can be submitted using the following command:"
            consolePrint "spark-submit.sh [submit-options] <app jar | python file> [app arguments] [--jsonout]"
            exit 1;
        fi
        apps=$(echo $result | sed 's/\"apps\"/\"apps\"\!/g' | cut -d \! -f2)
        tags=("apps")
        result="{[apps:$apps]}"
        printResult "$result" $tags
    fi
}

cluster_verify(){
    progress(){
        pid=$1
        bar="#"
        progress=0
        spin=("-" "\\" "|" "/")
        echo -ne "[($progress%)] Verifying: $bar ]\r"
        while kill -0 $pid 2> /dev/null; do
#            if [ $progress -ge 98 ]; then
#                val=0.5
#                bar="#"$bar
#                #sleep 4
            if [ $progress -ge 95 ]; then
                val=1
                bar="#"$bar
                wait $pid
            elif [ $progress -ge 90 ]; then
                val=1
                bar="##"$bar
            elif [ $progress -ge 80 ]; then
                val=2
                bar="###"$bar  
            elif [ $progress -ge 60 ]; then
                val=5
                bar="####"$bar
            else
                val=10
                bar="#####"$bar
            fi

            progress=$(expr $progress + $val)
            for s in ${spin[@]}
            do
                echo -ne "[($progress%):$s] Verifying: $bar ]\r"
                sleep 0.10
            done
            sleep 0.5
        done
        #Get process return code
        wait $pid
        status=$?
        if [ $status -ne 0 ];
        then
            echo -ne "\ndashDB-spark cluster verification failed. Please check the latest app logs for further details.\n"
            exit 1
        else
            sleep 1 #Wait for spark job to finish
            progress=$(expr $progress + $(expr 100 - $progress))
            echo -ne "[($progress%)] Verified: $bar ]\r\n"
            return 0
        fi
    }
    
    exec_verify(){
        header="Content-Type:application/json;charset=UTF-8"
        data='{"appResource" : "", "mainClass" : "com.ibm.idax.verify.ClusterVerify", "sparkProperties" : { }}'
        retcode=$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XPOST ${DASHDBURL}${SUBMIT_ENDPNT} --header "$header" --data "$data" -w "%{http_code}" -o /dev/null)
        if [ "$retcode" == "200" ];
        then
            return 0
        else
            return 1
        fi
    }

    exec_verify & progress $!
    ret=$?
    if [ $ret -eq 0 ];
    then
        curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XGET "${DASHDBURL}${LOGS_ENDPNT}/submission.out" | 
        awk '/BEGIN/{flag=1;next}/END/{flag=0} flag {print}' | less
    fi
}

#-----------------------OtherOperations-END-------------------------#

#-----------------------LogOperations-START-------------------------#
getlog_files(){
    endpoint=$1
    result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XGET $endpoint)" 

    files=''
    IFS=','
    for field in $result
    do
        result="$(echo $field | grep 'result' )"
        if [ ! -z $result ];
        then
            files=$(echo $result | cut -d \: -f2)
            break;
        fi
    done

    echo $files
}

getLogs(){
    logtyp=$1
    endpoint=""
    folder=""
    if [ $logtyp == "cluster" ];
    then
        clustId=$2
        folder="cluster_"$clustId
        endpoint="${DASHDBURL}${LOGS_FOLDER_ENDPNT}/cluster_$clustId"
    elif [ $logtyp == "app" ];
    then
        subid=$2
        if [ "" == "$subid" ];
        then
            endpoint="${DASHDBURL}${LOGS_ENDPNT}"
            folder="log_latest"
        else
            endpoint="${DASHDBURL}${LOGS_FOLDER_ENDPNT}/submission_$subid"
            folder="log_$subid"
        fi
    fi

    curdir=""
    if [ "$DOWNLOAD_TARGET" != "" ];
    then
        DOWNLOAD_TARGET=${DOWNLOAD_TARGET%/} #Formatting Path:-Removes the last '/' if present
        curdir=$DOWNLOAD_TARGET
    else
        curdir="$(pwd)"
    fi

    files=$(getlog_files $endpoint)
    IFS=" "
    logtyp=($(echo $files | cut -d \" -f2 | sed 's;\.\\\/;;g' | sed 's;\\n; ;g'))
    logcnt=$(echo ${#logtyp[@]})
    incr=$(expr 100 / $logcnt)
    progress=0
    bar="="
    echo -ne "[($progress%)] Downloading: $bar> ]\r"
    for log in ${logtyp[@]}
    do
        result_code="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -w "%{http_code}" -XGET $endpoint/$log -o $curdir/$folder/$log)" 
        if [ "$result_code" == "200" ];
        then
            bar="========"$bar
            progress=$(expr $progress + $incr)
            echo -ne "[($progress%)] Downloading: $bar> ]\r"
        elif [ "$result_code" == "400" ];
        then
            error="$(cat $curdir/$folder/$log)"
            tags=("resultCode")
            echo -ne "[($progress%)] Downloading: $bar> ]\n"
            file_on_server="log/$(basename $endpoint)/$log"
            printResult "$error" $tags "download" "$file_on_server"
            rm -rf $curdir/$folder/$log
            exit 1
        fi
    done
    
    progress=$(expr $progress + $(expr 100 - $progress))
    bar="="$bar
    echo -ne "[($progress%)] Downloading: $bar>]\n"
    result='{"resultCode":"SUCCESS"}' #Construct the SUCCESS result message to provide to the printResult function
    tags=("resultCode")
    printResult $result $tags "download" "${curdir}/$folder"

}
#-----------------------LogOperations-END-------------------------#
#-----------------------Handle InputParameters-START-------------------------#

  #Only for Other option and not for submit option
validate_parmcount(){
    parmcnt_actual=$1
    parmcnt_expected=$2 #including jsonout
    
    if [[ ( $parmcnt_actual -ne $parmcnt_expected ) ]];
    then
        consolePrint "Syntax error. Too many inputs provided."
        printHelp "usage" 1
    fi
}

validate_additional_parm(){
    parmcnt=$#
    usr=""
    usr_parm_exists="FALSE"
    target_path=""
    target_parm_exists="FALSE"
    
    if [ "$1" == "--user" ];
    then
        if [ -z $2 ]; 
        then                            
            consolePrint "Syntax error. Invalid input(s) provided."
            printHelp "usage" 1
        else
            usr="$2"
            usr_parm_exists="TRUE"
            shift
            shift
        fi
    fi
    
    if [ "$1" == "--dir" ];
    then
        if [ -z $2 ]; 
        then                            
            consolePrint "Syntax error. Invalid input(s) provided."
            printHelp "usage" 1
        else
            target_path="$2"
            target_parm_exists="TRUE"
            shift
            shift
        fi
    fi
    
    cmd="$1"
    option="$2"
    while [[ $# > 0 ]];
    do
     input=$1
     case $input in
       --jsonout)
              export DASHDBJSONOUT=YES
              parmcnt=$(($parmcnt-1))
              shift
              ;;
        --user)
            if [[ ( ! -z $FILEOPUSER  ) || ( "$usr_parm_exists" == "TRUE" ) ]];
            then
                consolePrint "Syntax error. Too many inputs provided."
                printHelp "usage" 1
            else
                usr="$2"
                usr_parm_exists="TRUE"
            fi
            shift
            shift
            ;;
        --dir)
            target_path="$2"
            target_parm_exists="TRUE"
            shift
            shift
            ;;
            *)
            shift
            ;;
     esac
    done
    
    if [ "$usr_parm_exists" == "TRUE" ];
    then
        if [[ ( "$cmd" != "--upload-file" &&  
              "$cmd" != "--download-file" && 
              "$cmd" != "--list-files" && 
              "$cmd" != "--delete-file" ) ]];
        then
            consolePrint "Syntax error. --user option can be used only by an administrator, and applies only to the"`
                        `" --upload-file, --download-file, --upload-file and --list-file commands."
            printHelp "usage" 1
            export PARMUSER=
            exit 1
        elif [ "$option" == "globallibs" ];
        then
            consolePrint "Syntax error. --user option can be used only by an administrator, and applies only to"`
                        `" user specific folders and not globallibs."
            printHelp "usage" 1
            export PARMUSER=
            exit 1
        else
#            validateUser "$usr"
            export PARMUSER=$usr
            parmcnt=$(($parmcnt-2))
        fi
    fi
    
    if [ "$target_parm_exists" == "TRUE" ];
    then
        if [[ ( "$cmd" != "--download-file" ) && 
              ( "$cmd" != "--download-app-logs" ) && 
              ( "$cmd" != "--download-cluster-logs" )  ]];
        then
            consolePrint "Syntax error. Invalid input(s) provided."
            consolePrint "  --dir option can be used only with download options: --download-file, --download-app-logs & --download-cluster-logs."
            export DOWNLOAD_TARGET=
            exit 1
        elif [ "$target_path" == "" ];
        then
            consolePrint "Syntax error. Please provide a valid download target path."
            export DOWNLOAD_TARGET=
            exit 1
        elif [ ! -d $target_path ];
        then
            consolePrint "Syntax error. Invalid download target path provided."
            export DOWNLOAD_TARGET=
            exit 1
        elif [[ ( ! -w $target_path ) || ( ! -r $target_path ) || ( ! -x $target_path ) ]];
        then
            consolePrint "Target directory does not have necessary permissions."
            exit 1
        else    
            target_path=${target_path%/} #Formatting Path:-Removes the last '/' if present
            if [ "$target_path" == "."  ];
            then
                export DOWNLOAD_TARGET="$(pwd)"
            elif [ "$target_path" == ".." ];
            then
                export DOWNLOAD_TARGET="$(dirname $(pwd))"
            else
                export DOWNLOAD_TARGET="$target_path"
            fi
            parmcnt=$(($parmcnt-2))
        fi
    fi
    
    return $parmcnt
}
#-----------------------Handle InputParameters-END-------------------------#

#-----------------------Handle Input-START-------------------------#

sparksubmitexec(){

    if [[ $# == 0 ]]
    then
        printHelp "usage" 1;
    fi

    result=""
    input=$1
    
    case $input in
        --kill)
            subid=$2
            tags=()
            if [ -z $subid ];
            then
                consolePrint "Provide the Spark Application submission ID"
                consolePrint "    spark-submit.sh --kill <submission_id> [--jsonout]"
                exit 1;
            else
                if [[ "${subid}" =~ [^a-zA-Z0-9] ]]; 
                then 
                    tags=("resultCode" "message")
                else
                    tags=("status" "statusDesc")
                fi
                
                validate_additional_parm $@
                parmcnt=$?
                validate_parmcount $parmcnt 2
                validate_environment
                result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -X POST ${DASHDBURL}${CANCELJOB_ENDPNT}$subid)"
                
                printResult "$result" $tags
            fi
            ;;
        --app-status)
            subid=$2
            tags=()
            if [[ ( -z $subid ) || ( $subid == "--jsonout" ) ]];
            then
                consolePrint "Provide the Spark Application submission ID"
                consolePrint "    spark-submit.sh --app-status <submission_id> [--jsonout]"
                exit 1;
            else
                typ=""
                if [[ "${subid}" =~ [^a-zA-Z0-9] ]]; 
                then
                    typ=""
                    tags=("resultCode" "message")
                else
                    typ="status"
                    tags=("status" "statusDesc" "applicationId" "code" "message")
                fi
                
                validate_additional_parm $@
                parmcnt=$?
                validate_parmcount $parmcnt 2
                validate_environment
                result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XGET ${DASHDBURL}${SUBSTATUS_ENDPNT}$subid)"
                printResult "$result" $tags $typ
            fi
            ;;
        --cluster-status)
            result=""
            tags=()
#            if [[ ( ! -z $2 ) && ( "$2" != "--jsonout" ) ]];
#            then
#                consolePrint "Provided parameter $2 to get cluster status is invalid"
#                consolePrint "    spark-submit.sh --cluster-status [--jsonout]"
#                exit 1;
#            fi
            validate_additional_parm $@
            parmcnt=$?
            validate_parmcount $parmcnt 1
            validate_environment
            if [ $ISADMIN == "TRUE" ];
            then
                result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XGET ${DASHDBURL}${STATUS_ENDPNT}'?username=*')"
                tags=("status" "statusDesc" "clusters")
            elif [ $ISADMIN == "FALSE" ];
            then
                result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XGET ${DASHDBURL}${STATUS_ENDPNT})"
                tags=("status" "statusDesc" "running_apps")
            fi
            printResult "$result" $tags
            ;;
        --load-samples)
#            if [[ ( ! -z $2 ) && ( "$2" != "--jsonout" ) ]];
#            then
#                consolePrint "Provided parameter $2 to load example files is invalid"
#                consolePrint "    spark-submit.sh --load-samples [--jsonout]"
#                exit 1;
#            fi
            validate_additional_parm $@
            parmcnt=$?
            validate_parmcount $parmcnt 1
            validate_environment
            result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XPOST ${DASHDBURL}${LOAD_ENDPNT})"
            tags=("status" "statusDesc")
            printResult "$result" $tags
            ;;
        --deallocate)   
             validate_additional_parm $@
             parmcnt=$?
            validate_parmcount $parmcnt 1
            validate_environment
            result="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XPOST ${DASHDBURL}${DEALLOC_ENDPNT})"
            tags=("status" "statusDesc")
            printResult "$result" $tags
            ;;
        --webui-url)
            validate_parmcount $# 1
            validate_environment
            url="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -XGET ${DASHDBURL}${STATUS_ENDPNT} | sed -e $'s/,/\\\n/g' | grep -e monitoring_url)"
            if [ ! -z $url ];
            then
                tmp="$(echo $(echo $url | cut -d \: -f2)":"$(echo $url | cut -d \: -f3)":"$(echo $url | cut -d \: -f4))"
                weburl="$(echo $tmp | cut -d \" -f2 | sed 's/\\//g')"
                consolePrint "Web UI URL : $DASHDBURL$weburl"
            else
                consolePrint "The cluster is not running, so there is no web UI."
            fi
            ;;
        --display-app-log)
            type=$2
            subIds=""
            endpoint=""
            validateenv_and_getsubIds(){
                validate_environment
                subIds="$(getSubIds)"
            }
            if [[ ( -z ${type} ) || ( "${type}" != "info" && "${type}" != "err" && "${type}" != "out" && "$type" != "app" ) ]]; 
            then
                consolePrint "Provide the log type (err, out, app or info) to be displayed"
                consolePrint "spark-submit.sh --display-app-log <err|out|app|info> [submission_id]"
                exit 1;
            elif [[ ( -z $3 ) && ( "$type" == "info" || "$type" == "err" || "$type" == "out" || "$type" == "app" ) ]];
            then
                validate_parmcount $# 2
                validateenv_and_getsubIds
                if [ "${subIds}" == "" ];
                then
                    consolePrint "No error log present to display. Type=$type"
                    exit 1;
                fi
                endpoint="${LOGS_ENDPNT}"
            elif [[ ( "$type" == "info" || "$type" == "err" || "$type" == "out" || "$type" == "app") ]];
            then
                validate_parmcount $# 3
                validateenv_and_getsubIds
                subid=$3
                id=$(echo -e $subIds | grep -e $subid)
                
                if [ "$id" == "" ];
                then
                    consolePrint "No error log present to display. Type=$type, SubmissionID=$subid"
                    exit 1;
                else
                    endpoint="${LOGS_FOLDER_ENDPNT}/submission_$subid"
                fi
            fi 
            
            file=""
            files=$(getlog_files ${DASHDBURL}$endpoint)
            IFS=" "
            logtyp=($(echo $files | cut -d \" -f2 | sed 's;\.\\\/;;g' | sed 's;\\n; ;g'))
            for log in ${logtyp[@]}
            do
                logfile=$(echo $log | grep "$type")
                if [ ! -z $logfile ];
                then
                   file="$logfile" 
                   break 1
                fi
            done
            
            file_endpoint="$endpoint/$file"
            result_code="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -w "%{http_code}" -XGET ${DASHDBURL}$file_endpoint -o /dev/null)"
            if [ "$result_code" == "200" ];
            then
                cmd="curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -X GET ${DASHDBURL}$file_endpoint | less"
                eval $cmd
            else
                if [ -z $3 ];
                then
                    consolePrint "No error log present to display. Type=$type"
                else
                    subid=$3
                    consolePrint "No error log present to display. Type=$type, SubmissionID=$subid"
                fi
                exit 1
            fi 
            ;;
        --download-app-logs)
            subid=""
            dir=""
            logfolder=""
            subIds=""
            
            validateenv_and_subIds(){
                validate_environment
                subIds="$(getSubIds)"
                if [ "${subIds}" == "" ];
                then
                    consolePrint "No spark application submission logs present on the dashDB system for download."
                    exit 1;
                fi
            }

            validate_additional_parm $@
            parmcnt=$?
            if [ $parmcnt -gt 1 ];
            then
                subid="$2"
            fi
            
            if [ "$subid" == "" ];
            then
                validate_parmcount $parmcnt 1
                validateenv_and_subIds
                logfolder="log_latest"
            else
                validate_parmcount $parmcnt 2
                validateenv_and_subIds
                
                subIds=$(echo $subIds | sed 's# #\\n#g')
                id="$(echo -e $subIds | grep -w $subid)"
                
                if [ "$id" == "" ];
                then
                    consolePrint "No error log present to download. SubmissionID=$subid"
                    exit 1;
                else
                    logfolder="log_$subid"
                fi
            fi
            
            if [ "$DOWNLOAD_TARGET" != "" ];
            then
                DOWNLOAD_TARGET=${DOWNLOAD_TARGET%/} #Formatting Path:-Removes the last '/' if present
                dir=$DOWNLOAD_TARGET
            else
                dir="$(pwd)"
            fi

            checkfileexists "local" $logfolder
            exists=$?
            if [ $exists -eq 1 ];
            then
                exit 1
            fi
            rm -rf $dir/$logfolder && mkdir $dir/$logfolder
            getLogs "app" "$subid"
            ;;
        --display-cluster-log)
            type=$2
            hosttyp=$3
            hostip=$4
            endpoint=""
                        
            if [[ ( -z $type ) || ( "$type" != "err" && "$type" != "out" ) ]]; 
            then
                consolePrint "Provide the log type (err|out) to be displayed"
                consolePrint "spark-submit.sh --display-cluster-log <err|out> <master|worker> [worker-host-ip]"
                exit 1;
            elif [[ ( -z $hosttyp ) || ( "$hosttyp" != "master" && "$hosttyp" != "worker" ) ]];
            then
                consolePrint "Provide the process type (master|worker) to be displayed"
                consolePrint "spark-submit.sh --display-cluster-log <err|out> <master|worker> [worker-host-ip]"
                exit 1;
            else
                if [[ ( "$hosttyp" == "worker" ) && ( "$hostip" == "" ) ]];
                then
                    consolePrint "Provide the worker host to display the worker $type log"
                    consolePrint "spark-submit.sh --display-cluster-log <err|out> <master|worker> [worker-host-ip]"
                    exit 1;
                fi
                if [ "$hosttyp" == "master" ];
                then
                    validate_parmcount $# 3
                elif [ "$hosttyp" == "worker" ];
                then
                    validate_parmcount $# 4
                fi
                
                validate_environment
                clustId=$(getClusterId)
                if [ "$clustId" == "" ];
                then
                    consolePrint "No cluster logs present to display. Type=$type"
                    exit 1;
                fi
                endpoint="${LOGS_FOLDER_ENDPNT}/cluster_"$clustId
            fi 
            
            file=""
            files=$(getlog_files ${DASHDBURL}$endpoint)
            IFS=" "
            logtyp=($(echo $files | cut -d \" -f2 | sed 's;\.\\\/;;g' | sed 's;\\n; ;g'))
            for log in ${logtyp[@]}
            do
                logfile=$(echo $log | grep "$hosttyp" | grep "$type")
                
                if [ ! -z $logfile ];
                then
                    ip=$(echo "$logfile" | cut -d \_ -f2 | sed "s/.$type//g") 
                    if [[ ( "$hosttyp" == "worker" && "$ip" == "$hostip" ) || ( "$hosttyp" == "master" ) ]];
                    then
                        file="$logfile" 
                        break 1
                    fi
                fi
            done
            
            printerr(){
                if [ "$hosttyp" == "master" ];
                then
                    consolePrint "No error log present to display. Type=$type"
                elif [ "$hosttyp" == "worker" ];
                then
                    consolePrint "No error log present to display. Type=$type, Host=$hostip"
                fi
                exit 1
            }
            
            if [ "$file" == "" ];
            then
                printerr
            else
                file_endpoint="$endpoint/$file"
                result_code="$(curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -w "%{http_code}" -XGET ${DASHDBURL}$file_endpoint -o /dev/null)"
                if [ "$result_code" == "200" ];
                then
                    cmd="curl ${SS_CURL_OPTIONS} ${DASHDBUSER}:${DASHDBPASS} -X GET ${DASHDBURL}$file_endpoint | less"
                    eval $cmd
                else
                    printerr
                fi
            fi
            ;;
        --download-cluster-logs)
            validate_additional_parm $@
            parmcnt=$?
            validate_parmcount $parmcnt 1  
            validate_environment
            
            clustId=$(getClusterId)
            if [ "$clustId" == "" ];
            then
                consolePrint "No cluster log present on the dashDB system."
                exit 1
            fi
            logfolder="cluster_"$clustId
            
            dir=""
            if [ "$DOWNLOAD_TARGET" != "" ];
            then
                DOWNLOAD_TARGET=${DOWNLOAD_TARGET%/} #Formatting Path:-Removes the last '/' if present
                dir=$DOWNLOAD_TARGET
            else
                dir="$(pwd)"
            fi
            checkfileexists "local" $logfolder
            exists=$?
            if [ $exists -eq 1 ];
            then
                exit 1
            fi
            rm -rf $dir/$logfolder && mkdir $dir/$logfolder
            getLogs "cluster" "$clustId"
            ;;
        --list-apps)
            validate_additional_parm $@
            parmcnt=$?
            validate_parmcount $parmcnt 1
            validate_environment
            list_submittedapps
            ;;

        --upload-file)
            if [ -z $2 ];
            then
                consolePrint "Provide appropriate inputs to upload the file."
                consolePrint "    spark-submit.sh --upload-file [apps|defaultlibs|globallibs] <file> [--jsonout] [--user <user_name>]"
                exit 1;
            else 
                validate_additional_parm $@
                parmcnt=$?
                if [[ ( -z $3 ) || ( "$3" == "--jsonout" ) || ( "$3" == "--user" ) ]];
                then
                    validate_parmcount $parmcnt 2
                    uploadfile $2
                else
                    validate_parmcount $parmcnt 3
                    uploadfile $2 $3
                fi
            fi
            ;;
        --download-file)
            if [ -z $2 ];
            then
                consolePrint "Provide appropriate inputs to download the file."
                consolePrint "    spark-submit.sh --download-file [apps|defaultlibs|globallibs] <file> [--dir <target_path>] [--jsonout] [--user <user_name>]"
                exit 1;
            else 
                validate_additional_parm $@
                parmcnt=$?
                if [[ ( -z $3 ) || ( "$3" == "--jsonout" ) || ( "$3" == "--user" ) || ( "$3" == "--dir" ) ]];
                then
                    validate_parmcount $parmcnt 2
                    downloadfile $2
                else
                    validate_parmcount $parmcnt 3
                    downloadfile $2 $3
                fi
            fi
            ;;
        --list-files)
            validate_additional_parm $@
            parmcnt=$?
            if [[ ( -z $2 ) || ( "$2" == "--jsonout" ) || ( "$2" == "--user" ) ]];
            then
                validate_parmcount $parmcnt 1
                listfiles
            else
                validate_parmcount $parmcnt 2
                listfiles $2
            fi
            ;;
        --delete-file)
            if [ -z $2 ];
            then
                consolePrint "Provide appropriate inputs to delete the file."
                consolePrint "    spark-submit.sh --delete-file  [apps|defaultlibs|globallibs] <file> [--jsonout] [--user <user_name>]"
                exit 1;
            else
                validate_additional_parm $@
                parmcnt=$?
                if [[ ( -z $3 ) || ( "$3" == "--jsonout" ) || ( "$3" == "--user" ) ]];
                then
                    validate_parmcount $parmcnt 2
                    deletefile $2
                else
                    validate_parmcount $parmcnt 3
                    deletefile $2 $3
                fi
            fi
            ;;        
        --env)
            validate_parmcount $# 1
            display_env_variables
            ;;
        --verify)
            if [ ! -z $2 ];
            then
                consolePrint "Syntax error. Invalid input(s) provided."
                printHelp "usage" 1
            fi
            validate_environment
            cluster_verify
            ;;
        --jsonout)
            shift #This shift is to check if further parameter follow --json
            if [ -z $1 ];
            then
                consolePrint "Syntax error. Invalid input(s) provided."
                printHelp "usage" 1
            elif [ ! -z $1  ];
            then
                export DASHDBJSONOUT=YES
                sparksubmitexec $@            
            fi
            ;;
        --user|--dir)
            parmcnt=$#
            if [[( -z $2 || $parmcnt -le 2 )]]; #First check if --user is followed by parameter 
            then                            #and then check if they are the only 2 parameters
                consolePrint "Syntax error. Invalid input(s) provided."
                printHelp "usage" 1
            else
                validate_additional_parm $@
                shift
                shift
                sparksubmitexec $@
            fi           
            ;;
        --version)
            validate_parmcount $# 1
            consolePrint "$VERSION"
            ;;
        --help)
            validate_parmcount $# 1
            printHelp "all" 0
            ;;
        *)  
            validate_additional_parm $@
            handleSubmit $@
            ;;

    esac
}
#-----------------------Handle Input-END-------------------------#

#-----------------------dashDB-spark-cli-START-------------------------#
printDisclaimer $@
setenviron
sparksubmitexec $@
#-----------------------dashDB-spark-cli-END-------------------------#
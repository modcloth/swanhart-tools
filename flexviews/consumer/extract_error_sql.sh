#!/bin/bash

source $HOME/.profile

usage() {

cat << EOF

Valid options are:

-f full path & file name of log file

EOF
}

while getopts "hf:" flag; do
    case $flag in
          h) usage
             exit_success
             ;;
          f) log_file_name=$OPTARG;;
          *) echo "ERROR! "
             usage
             exit_failed
             ;;
    esac
done

#---- Main -------

if [ -z "$log_file_name" ] 
then
   usage
   exit 1
fi

if ! [ -f $log_file_name ]
then
   echo "File not found."
   exit 1
fi

echo -e "\nERROR SQL from: $log_file_name\n"

while read -r log_line
do
  repl_uow_id="${log_line/*@fv_uow_id=/}"
  temp_sql="${log_line/@fv_uow_id=*/;}"
  exec_sql="${temp_sql/@fv_uow_id/$repl_uow_id}"
  echo "$exec_sql"
done < <(grep -A1 "#ERROR STATEMENT" ${log_file_name} | grep -v "#ERROR STATEMENT")

echo ""

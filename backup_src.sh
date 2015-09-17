#!c:\bin\bash
#---------------------------------------------------------------------------

schema=${1}

BIN='c:/bin'
PYTHON_HOME='c:/python27'
BIN_LOCAL='c:/bin/db_2'


if   [ -z "${schema}" ];  then
    exit  1
fi

backup_dir='backup_src'
tmp_dir='tmp'

files=`${BIN}/find . | grep -i '.pd*'`

${BIN}/rm -f ${tmp_dir}/backup_objects.lst 2>/dev/null
${BIN}/mkdir ${tmp_dir} 2>/dev/null
${BIN}/mkdir ${backup_dir} 2>/dev/null


for  ff  in  ${files};  do
    grep -i "create or replace" ${ff} | awk '
           BEGIN  {$name = ""; $type = "";}

           {
               if   (toupper( $5 ) == "BODY")  {
                   name = $6;
                   type = $4" "$5;
               }
               else  {
                   name = $5;
                   type = $4;
               }
               print  type"\t"name;
           }'                        | tr "(" " " >> ${tmp_dir}/backup_objects.lst

done

cat  ${tmp_dir}/backup_objects.lst | ${PYTHON_HOME}/python ${BIN_LOCAL}/process_list.py --schema=${schema} --backup-dir=${backup_dir}


#! /usr/bin/env python
#------------------------------------------------------


import sys, re, getpass, os.path

try:
    import  cx_Oracle

except   ImportError, info:
    print  "Import Error:", info
    sys.exit( 2 )


db_schema_alias = None
cmd_file = None


db_schema = None
db_conn = None
db_tns = None
db_pass = None
os_username = None
patch_number = None

db_secrets = "C:\\bin\\Db\\oracle.pwl"


# -------------------------------------------------------------------------------------------------

def  GetDBcredentials( db_alias ):

    file = open( db_secrets )
    for  _line  in  iter( file ):
        mo_alias = re.match("\s*" + db_alias + "\s*=\s*", _line, re.IGNORECASE)
        if  mo_alias:
            # print  _line
            _list = _line.split("=")
            tmp_val = _list[1]
            _list = tmp_val.split("/")
            db_schema = _list[0].upper().strip(' \t\n\r')
            db_pass = _list[1]
            _list = db_pass.split("@")
            db_pass =_list[0].strip(' \t\n\r')
            db_tns = _list[1].upper().strip(' \t\n\r')

            return  db_schema, db_pass, db_tns


# -------------------------------------------------------------------------------------------------

def  SaveData( o_type, o_name, o_operation, o_filename ):

    print o_type, o_name, o_operation, o_filename
    # return

    _is_created = 0

    sql = " SELECT  count(*)  FROM  user_objects  WHERE  object_name = upper( :name ) "
    curs = db_conn.cursor()
    curs.execute(sql, name=o_name )
    row = curs.fetchone()
    if  row[0] == 0:
        _is_created = 1
    curs.close()

    sql = """
        INSERT INTO  lck_patch_log  (patch_numb, object_type, object_name, object_operation, is_created, os_user, patch_date, script_filename)
            VALUES  (:patch_numb, :object_type, :object_name, :object_operation, :is_created, :os_user, sysdate, :script_filename)
    """
    curs = db_conn.cursor()
    curs.execute(sql, patch_numb=patch_number, object_type=o_type, object_name=o_name, object_operation=o_operation,
                      is_created=_is_created,
                      os_user=os_username,
                      script_filename=o_filename)
    db_conn.commit()
    curs.close()


# -------------------------------------------------------------------------------------------------

def  ParseFile( fname ):

    file = open( fname )
    for  _line  in  iter( file ):
        obj_type = ""
        obj_name = ""
        obj_operation = None

        mo_create = re.match("\s*create\s*\w+\s*", _line, re.IGNORECASE)

        if  mo_create:
            mo_replace = re.match("\s*(.*)or\s*replace\s*(.*)\s*", _line, re.IGNORECASE)
            if  mo_replace:
                obj_operation = "CREATE OR REPLACE"
                mo_type = re.match("\s*(.*)or\s*replace\s*(\w+)\s*(\w+)", _line, re.IGNORECASE)
                obj_type = mo_type.group(2).upper().strip(' \t\n\r')
                next_word = mo_type.group(3).upper().strip(' \t\n\r')
                if  obj_type == "PACKAGE":
                    mo_package_body = re.match("\s*(.*)body\s*(\w+)\s*(\w*)", _line, re.IGNORECASE)
                    if  mo_package_body:
                        obj_type = "PACKAGE BODY"
                        obj_name = mo_package_body.group(2).upper().strip(' \t\n\r')
                    else:
                        obj_name = next_word
                elif  obj_type == "TYPE":
                    mo_type_body = re.match("\s*(.*)body\s*(\w+)\s*(\w*)", _line, re.IGNORECASE)
                    if  mo_type_body:
                        obj_type = "TYPE BODY"
                        obj_name = mo_type_body.group(2).upper().strip(' \t\n\r')
                    else:
                        obj_name = next_word
                else:
                    obj_name = next_word

                # print  "create or replace ---> " + obj_type + "| ---> |" + obj_name + "|"

            else:   #-- create without replace
                mo_type = re.match("\s*create\s*(\w+)\s*(\w+)\s*", _line, re.IGNORECASE)
                obj_type = mo_type.group(1).upper().strip(' \t\n\r')
                obj_name = mo_type.group(2).upper().strip(' \t\n\r')
                # print  "create ---> " + obj_type + "| ---> |" + obj_name + "|"
                obj_operation = "CREATE"

        else:
            mo_alter = re.match("\s*alter\s*(\w+)\s*(\w+)\s*", _line, re.IGNORECASE)
            if  mo_alter:
                obj_type = mo_alter.group(1).upper().strip(' \t\n\r')
                obj_name = mo_alter.group(2).upper().strip(' \t\n\r')
                # print  "alter ---> " + obj_type + "| ---> |" + obj_name + "|"
                obj_operation = "ALTER"

            mo_drop = re.match("\s*drop\s*(\w+)\s*(\w+)\s*", _line, re.IGNORECASE)
            if  mo_drop:
                obj_type = mo_drop.group(1).upper().strip(' \t\n\r')
                obj_name = mo_drop.group(2).upper().strip(' \t\n\r')
                # print  "drop ---> " + obj_type + "| ---> |" + obj_name + "|"
                obj_operation = "DROP"

        if  obj_operation:
            SaveData( obj_type, obj_name, obj_operation, fname )

    file.close()

    return



# ------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":


    try:
        cmd_file = open(str(sys.argv[1]))
    except:
        print  "Can not open cmd/sql-file: " + str(sys.argv[1])
        sys.exit( 1)

    db_schema_alias = str(sys.argv[2])

    db_schema, db_pass, db_tns = GetDBcredentials( db_schema_alias )


    db_conn = cx_Oracle.connect( db_schema, db_pass, db_tns )

    os_username = getpass.getuser()
    mo_patch = re.search( r"(.*)\\(\d*)[\-\_](\d*)(.*)", os.path.abspath(os.path.join(os.pardir)) )
    if  not mo_patch:
        mo_patch = re.search( r"(.*)\\(\d*)[\-\_](\d*)(.*)", os.path.abspath(os.path.join(os.pardir) + "/../") )


    patch_number = mo_patch.group(2) + "-" + mo_patch.group(3)


    ParseFile(str(sys.argv[1]))

    row = cmd_file.readlines()

    for line in row:
        line = line.strip('\r')
        line = line.strip('\n')

        if (line.find("@") > -1):
            f_script_name = line.split("@")
            # print  f_script_name[1]
            f_script_name[1] = f_script_name[1].strip(';')
            ParseFile(f_script_name[1])


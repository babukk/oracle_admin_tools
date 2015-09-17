#! env python
# -------------------------------------------------------------------------------------------------------
# -*- coding: cp1251 -*-
# -------------------------------------------------------------------------------------------------------

import  os
import  sys
import  getopt

try:
    import  cx_Oracle

except   ImportError, info:
    print  "Import Error:", info
    sys.exit( 2 )



# -------------------------------------------------------------------------------------------------------

def  extract_view( _conn, _obj_name ):

    _cursor = _conn.cursor()

    _cursor.execute( """
               SELECT  text  FROM  user_views  WHERE  view_name = :name
    """, name = _obj_name )

    _result = _cursor.fetchall()

    if   (_result):
        sys.stdout.write( "CREATE OR REPLACE VIEW  " + _obj_name + "  AS\n" )
        for  _text  in  _result:
            sys.stdout.write( _text[0] )

        print  ";"
        print  "/"


# -------------------------------------------------------------------------------------------------------

def  extract_source( _conn, _obj_type, _obj_name ):

    _cursor = _conn.cursor()

    _cursor.execute( """

              SELECT  text
                   FROM  user_source
                        WHERE  name = UPPER( :obj_name )
                          AND  type = UPPER( :obj_type )
                     ORDER BY  line

    """, obj_type = _obj_type,
         obj_name = _obj_name )

    _result = _cursor.fetchall()

    if   (_result):
        sys.stdout.write( "CREATE OR REPLACE  " )
        for  _text  in  _result:
            sys.stdout.write( _text[0] )

        print
        print  "/"



# -------------------------------------------------------------------------------------------------------

def  main():

    sys.path.append( "C:\\bin\\Db_2" )
    os.environ['NLS_LANG'] = "american_america.CL8MSWIN1251"


    try:
        opts, args = getopt.getopt( sys.argv[1:], "ht:n:d:p:s:", ["help", "type=", "name=", "db=", "pass=", "schema="] )

    except:
        sys.exit( 1 )

    for  oo, a in opts:
        if   oo  in  ("-h", "--help"):
            print( "usage: python extract_source.py  -t|--type=OBJECT_TYPE -n|--name=OBJECT_NAME" )
            sys.exit( 0 )
        elif  oo  in  ("-t", "--type"):
            obj_type = a
        elif  oo  in  ("-n", "--name"):
            obj_name = a
        elif  oo  in  ("-u", "--db"):
            _db = a
        elif  oo  in  ("-p", "--pass"):
            _pass = a
        elif  oo  in  ("-s", "--schema"):
            _schema = a

        else:
            assert False, "unhandled option"

    try:
        conn = cx_Oracle.connect( _schema, _pass, _db )

    except  cx_Oracle.DatabaseError, info:
        print  "Logon  Error:", info
        sys.exit( 1 )

    if   (obj_type.upper() == 'VIEW'):
        extract_view( _conn = conn, _obj_name = obj_name )
    else:
        extract_source( _conn = conn, _obj_type = obj_type, _obj_name = obj_name )

# =======================================================================================================

if   __name__ == "__main__":
    main()


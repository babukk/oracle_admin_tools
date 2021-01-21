
import  sys
import  os
import  getopt
import  re

try:
      opts, args = getopt.getopt(sys.argv[1:], "hs:b:", ["help", "schema=", "backup-dir="])

except:
      sys.exit(1)

db_secrets = "C:\\bin\\Db\\oracle.pwl"


def  GetDBcredentials(db_alias):

    file = open(db_secrets)
    for  _line  in  iter(file):
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


for  oo, a  in  opts:
    if   oo  in  ("-h", "--help"):
        print("usage: python .py process_list.py -s|--schema=SCHEMA -b|backup-dir=DIR")
        sys.exit(0)
    elif  oo  in  ("-s", "--schema"):
        _schema = a

    elif  oo  in  ("-b", "--backup-dir"):
        _backup_dir = a

    else:
        assert False, "unhandled option"


dump_file = open(_backup_dir + "\\dump.sql", "w+")

_schema, _pass, _db = GetDBcredentials(_schema)

dump_file.write("------------------------------------------------------\n")
dump_file.write("-- Export file for user " + _schema + "                      --\n")
dump_file.write("------------------------------------------------------\n")
dump_file.write("\nspool dump.log\n")

for  _line  in  sys.stdin.readlines():
    (_type, _name) = _line.split('\t')
    _name = _name.rstrip('\n')
    _name = _name.replace('"', '')
    try:  (_name, _xxx) = _name.split(' ')
    except:  pass
    _name = _name.rstrip(' ')
    _type = _type.upper()
    _name = _name.upper()

    if   (_type == 'PACKAGE'):
        _f_ext = 'spc'

    elif   (_type == 'PACKAGE BODY'):
        _f_ext = 'bdy'

    elif   (_type == 'PROCEDURE'):
        _f_ext = 'prc'

    elif   (_type == 'FUNCTION'):
        _f_ext = 'fnc'

    elif   (_type == 'VIEW'):
        _f_ext = 'vw'

    elif   (_type == 'TYPE'):
        _f_ext = 'tps'

    elif   (_type == 'TYPE BODY'):
        _f_ext = 'tpb'

    elif   (_type == 'TRIGGER'):
        _f_ext = 'trg'

    elif   (_type == 'SEQUENCE'):
        _f_ext = 'seq'

    else:
        _f_ext = 'sql'


    os.system("c:\\python27\\python c:\\bin\\db_2\\extract_source.py --db=" + _db +
                                                                  " --pass=" + _pass +
                                                                  " --schema=" + _schema +
                                                                  " --type=\"" + _type +
                                                                  "\" --name=\"" + _name +
                                                                  "\" 1>" + _backup_dir + "\\" + _name + "." + _f_ext)

    f_size = os.path.getsize(_backup_dir + "\\" + _name + "." + _f_ext)
    if   (f_size == 0):
        os.unlink(_backup_dir + "\\" + _name + "." + _f_ext)
    else:
        dump_file.write("prompt\n")
        dump_file.write("prompt Creating " + _type + " " + _name + "\n")
        dump_file.write("prompt =============================================\n")
        dump_file.write("prompt\n")
        dump_file.write("@@" + _name + "." + _f_ext + "\n")


dump_file.write("\nspool off\n")

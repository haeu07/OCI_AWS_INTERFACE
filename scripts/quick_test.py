import tools as t
import cx_Oracle


def quick_test_oracle():
    params = t.read_ini_file(filename='database.ini', section='dbotdsr2.rzsamgpp_int')
    or_connect_str = params['host']
    con = cx_Oracle.connect( params['user'], params['password'], or_connect_str )
    cur = con.cursor()

    run_sql = f"""select pt.product_type_key, pt.product_type_name
                  from   dig_product_type pt
                  where  pt.product_type_key like '1%' """
    cur.execute(run_sql)
    all_rows = cur.fetchall()

    for one_row in all_rows:
        print( one_row[0] + " " + one_row[1] )

    return


quick_test_oracle()

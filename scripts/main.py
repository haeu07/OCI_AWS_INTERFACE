import constants as c
import pp_db as db
import tools as t


def test_db( p_trans_id ):
    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id=1, log_text=f'first test here' )
    con = db.connect( run_id=p_trans_id )
    cur = db.open_cursor( con, run_id=p_trans_id )

    run_sql = f"""select t.trans_info_loc_key
                from pp_trans_control tc, pp_target_control t
                where tc.target_key = t.target_key
                  and tc.trans_id = {p_trans_id}"""
    db.execute(cur, run_sql, run_id=p_trans_id)
    a_row = db.fetchone(cur)
    print( f"""Hello DB: {a_row['trans_info_loc_key']}""")


def test_db_oci( p_trans_id ):
    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id=1, log_text=f'first test here' )
    con = db.connect( run_id=p_trans_id, ini_file_section='dbotdsr2.rzsamgpp_int' )
    cur = db.open_cursor( con, run_id=p_trans_id )


    run_sql = f"""select pt.product_type_key, pt.product_type_name
                from dig_product_type pt"""
    db.execute(cur, run_sql, run_id=p_trans_id)
    #cur.rowfactory = lambda *args: dict(zip([d[0] for d in cur.description], args))

    #a_row = db.fetchone(cur)
    all_rows = db.fetchall(cur)

    print( f"""Hello Oracle DB""")

    for one_row in all_rows:
        print( one_row['PRODUCT_TYPE_KEY'] + " " + one_row['PRODUCT_TYPE_NAME'] )


if __name__ == '__main__':
    #test_db( p_trans_id = 544 )
    test_db_oci( p_trans_id = 544 )

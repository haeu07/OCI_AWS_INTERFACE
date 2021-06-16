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


if __name__ == '__main__':
    test_db( p_trans_id = 544 )

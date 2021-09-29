import constants as c
import pp_db_oracle as db
import tools as t
import aws_message_q as q
import cx_Oracle
import requests, json


def quick_test_oracle():
    params = t.read_ini_file(filename='database.ini', section='dbotdsr2.rzsamgpp_int')
    or_connect_str = params['host']
    con = cx_Oracle.connect( params['user'], params['password'], or_connect_str )
    cur = con.cursor()

    run_sql = f"""select pt.product_type_key, pt.product_type_name
                  from   dig_product_type pt
                  where  pt.product_type_key like '1%' """
    cur.execute(run_sql)
    all_rows = db.fetchall(cur)

    for one_row in all_rows:
        print( one_row[0] + " " + one_row[1] )

    return


def test_db( p_run_id ):
    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id=1, log_text=f'first test here' )
    con = db.connect( run_id=p_run_id )
    cur = db.open_cursor( con, run_id=p_run_id )

    run_sql = f"""select t.trans_info_loc_key
                from pp_trans_control tc, pp_target_control t
                where tc.target_key = t.target_key
                  and tc.trans_id = {p_run_id}"""
    db.execute(cur, run_sql, run_id=p_run_id)
    a_row = db.fetchone(cur)
    print( f"""Hello DB: {a_row['trans_info_loc_key']}""")


# mix oci and aws access
def test_db_oci( p_run_id ):
    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id=1, log_text=f'first test here' )

    # open postgres
    con_pg = db.connect( run_id=p_run_id )
    cur_pg = db.open_cursor( con_pg, run_id=p_run_id )

    run_sql = f"""select t.trans_info_loc_key
                  from   pp_trans_control tc, pp_target_control t
                  where  tc.target_key = t.target_key
                  and    tc.trans_id = {p_run_id}"""
    db.execute(cur_pg, run_sql, run_id=p_run_id)
    a_row_pg = db.fetchone(cur_pg)


    # open oracle
    con_or = db.connect( run_id=p_run_id, ini_file_section='dbotdsr2.rzsamgpp_int' )
    cur_or = db.open_cursor( con_or, run_id=p_run_id )

    run_sql = f"""select pt.product_type_key, pt.product_type_name
                  from   dig_product_type pt
                  where  pt.product_type_key like '1%' """
    db.execute(cur_or, run_sql, run_id=p_run_id)
    all_rows = db.fetchall(cur_or)



    print( f"""Hello Postgres DB: {a_row_pg['trans_info_loc_key']}""")
    print( "-----------------------------------------------------------")
    print( f"""Hello Oracle DB""")

    for one_row in all_rows:
        print( one_row['PRODUCT_TYPE_KEY'] + " " + one_row['PRODUCT_TYPE_NAME'] )


def test_msg_q():
    msg_body = {
        'digital_report_id': 12345,     # the message body posted as a json
        'user_activity': 'approve',
        'approval_timestamp': '20210714 10:10:10'
    }

    q.send_to_pp_aws_message_q( p_run_id = 1, p_message_type_key = q.PP_AWS_MSG_TYPE_DPM_FILE_APPROVE, p_message_body = msg_body )
    return


def test_api():
    PP_AWS_MSG_Q_URL     = 'https://xdq824bdac.execute-api.us-east-1.amazonaws.com/prod/pp-aws-message-q'
    PP_AWS_MSG_Q_API_KEY = '27qsecNR7ea0jl7B09GL86YWs7B3419A2i8Hmu64'

    body = { 'source_system_key': 1,       # indicates source system where this message came from (1 = PreProc on OCI)
             'message_type_key': 1,        # what type of message is this (1 = DPM user file activity, 2 = ... )
             'message_body': {
                 'digital_report_id': 12345,     # the message body posted as a json
                 'user_activity': 'approve',
                 'approval_timestamp': '20210714 10:10:10' },
             'message_status_key': 1 }

    header = { 'x-api-key': PP_AWS_MSG_Q_API_KEY }

    print( header )
    print( body )

    response = requests.post(
        PP_AWS_MSG_Q_URL,
        data = json.dumps( body ),
        headers = header
    )

    print( response.json() )


if __name__ == '__main__':
    #q.dpm_file_approval( p_digital_report_id=111194 )

    # test_db( p_run_id = 544 )
    # test_db_oci( p_run_id = 544 )
    # test_msg_q()
    test_api()
    # quick_test_oracle()
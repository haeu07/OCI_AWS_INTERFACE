import json, sys, requests
import constants as c
import tools as t
import pp_db as db

PP_AWS_MSG_Q_URL     = 'https://xdq824bdac.execute-api.us-east-1.amazonaws.com/prod/pp-aws-message-q'
PP_AWS_MSG_Q_API_KEY = '27qsecNR7ea0jl7B09GL86YWs7B3419A2i8Hmu64'
PP_OCI_SOURCE_SYSTEM_KEY = 1

# Message Types
PP_AWS_MSG_TYPE_DPM_FILE_APPROVE = 1
PP_AWS_MSG_TYPE_DPM_FILE_REJECT  = 2
PP_AWS_MSG_TYPE_DPM_FILE_DELETE  = 3


def send_to_pp_aws_message_q( p_message_type_key, p_message_body, p_run_id = None ):
    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_run_id, log_text=f'call message type: {p_message_type_key} with message body: {p_message_body}' )

    # create parameters to be sent over the API
    params_dict={ 'source_system_key': PP_OCI_SOURCE_SYSTEM_KEY,       # indicates source system where this message came from (1 = PreProc on OCI)
                  'message_type_key': p_message_type_key,        # what type of message is this (1 = DPM user file activity, 2 = ... )
                  'message_body': p_message_body,
                  'message_status_key': 1
                  }
    try:

        response = requests.post(
            PP_AWS_MSG_Q_URL,
            data = json.dumps( params_dict ),
            headers={ 'x-api-key': PP_AWS_MSG_Q_API_KEY }
        )
    except requests.exceptions.RequestException as e:
        t.logger( log_type=t.LOG_TYPE_ERROR, run_id = p_run_id, log_text='Error sending post to AWS', traceback_info=sys.exc_info()[2] )
        raise SystemExit(e)

    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_run_id, log_text='call OK - response: ' + response.text  )
    return response.json()["body"] # returns new message_no created in table pp_message_queue@AWS



def dpm_file_approval( p_digital_report_id ):
    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_digital_report_id, log_text=f'start with digital_report_id: {p_digital_report_id} ' )

    con = db.connect( run_id=p_digital_report_id, ini_file_section=c.PP_DB_DEFAULT_INI_FILE_SECTION )
    cur = db.open_cursor( con, run_id=p_digital_report_id )

    run_sql = f"""select r.WHEN_APPROVED
                  from   digital_report r
                  where  r.digital_report_id = {p_digital_report_id}"""
    db.execute(cur, run_sql, run_id=p_digital_report_id)
    a_row = db.fetchone(cur)

    msg_body = {
        'digital_report_id': p_digital_report_id,
        'approval_timestamp': a_row['WHEN_APPROVED'].strftime( c.PP_DEFAULT_TIMESTAMP_FMT_PY )
    }

    message_no = send_to_pp_aws_message_q( p_message_type_key=PP_AWS_MSG_TYPE_DPM_FILE_APPROVE, p_message_body=msg_body, p_run_id = p_digital_report_id )
    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_digital_report_id, log_text=f'done - new message_no: {message_no} ' )
    return message_no
import json, sys, requests
import constants as c
import tools as t
import pp_db_oracle as db

PP_AWS_MSG_Q_URL     = 'https://starsd-api.scuba.tools/v1/aws/pp-aws-message-q'
PP_OCI_SOURCE_SYSTEM_KEY = 1
PP_MSG_Q_STATUS_INI = 10
PP_MSG_Q_STATUS_RUN = 20
PP_MSG_Q_STATUS_OK  = 30
PP_MSG_Q_STATUS_ERR = 40


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
                  'message_status_key': PP_MSG_Q_STATUS_INI
                  }

    try:
        params_json = json.dumps( params_dict )
        t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_run_id, log_text=f'complete json for the API call:: {params_json} ' )
    except:
        t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_run_id, log_text=f'complete json for the API call:: {params_json} ' )
        t.logger(log_type=t.LOG_TYPE_ERROR, run_id = p_run_id, log_text=f"parameter was not a dict that could be serialized into a json", traceback_info=sys.exc_info()[2])
        raise Exception( f"parameter was not a dict that could be serialized into a json" )


    try:
        params = t.read_ini_file(filename=c.PP_DB_DEFAULT_INI_FILE, section=c.API_KEY_INI_FILE_SECTION)
        response = requests.post(
            PP_AWS_MSG_Q_URL,
            data = params_json,
            headers={ 'x-api-key': params['x_api_key'] }
        )
    except requests.exceptions.RequestException as e:
        t.logger( log_type=t.LOG_TYPE_ERROR, run_id = p_run_id, log_text='Error sending post to AWS', traceback_info=sys.exc_info()[2] )
        raise SystemExit(e)

    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_run_id, log_text='call OK - response: ' + response.text  )
    return response.json()["body"] # returns new message_no created in table pp_message_queue@AWS



########################################################################################################################
# procedure:    dpm_activity
# parameters:   p_digital_report_id         DPM digital report id
#               p_status                    activity selected in DPM. Allowed values ['approve', 'reject', 'delete', 'refresh_ctl_report']
#               p_action                    option or the activity in DPM. Allowed values ['no_action', 'reload', 'skip_gras_match', 'none']
# description:  create the message about a DPM activity (approve/reject/delete) that is sent to AWS message queue so
#               that it can be executed there
########################################################################################################################

def dpm_activity( p_digital_report_id, p_status, p_action ):
    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_digital_report_id, log_text=f'start with digital_report_id: {p_digital_report_id} ' )

    con = db.connect( run_id=p_digital_report_id, ini_file_section=c.PP_DB_DEFAULT_INI_FILE_SECTION )
    cur = db.open_cursor( con, run_id=p_digital_report_id )

    run_sql = f"""select r.WHEN_APPROVED
                  from   digital_report r
                  where  r.digital_report_id = {p_digital_report_id}"""
    db.execute(cur, run_sql, run_id=p_digital_report_id)
    a_row = db.fetchone(cur)

    if a_row:
        if p_status.lower()  in ['approve', 'reject', 'delete', 'refresh_ctl_report']:
            if p_action.lower()  in ['no_action', 'reload', 'request_new_file','skip_gras_match', 'none']:
                msg_body = {
                    'digital_report_id': p_digital_report_id,
                    'status': p_status,
                    'action': p_action,
                    'approval_timestamp': a_row['WHEN_APPROVED'].strftime( c.PP_DEFAULT_TIMESTAMP_FMT_PY )
                }

                message_no = send_to_pp_aws_message_q( p_message_type_key=PP_AWS_MSG_TYPE_DPM_FILE_APPROVE, p_message_body=msg_body, p_run_id = p_digital_report_id )
                t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_digital_report_id, log_text=f'done - new message_no: {message_no} ' )
            else:
                message_no = -1
                t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_digital_report_id, log_text=f'invalid action: {p_action} so return -1' )
        else:
            message_no = -1
            t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_digital_report_id, log_text=f'invalid status: {p_status} so return -1' )
    else:
        message_no = -1
        t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = p_digital_report_id, log_text=f'no DPM record found for digital_report_id: {p_digital_report_id} so return -1' )

    db.close_cursor( cur )
    db.close( con )
    return message_no
import tools as t
import aws_message_q as q
import sys

# get call argument
try:
    digital_report_id = sys.argv[1]
    status = sys.argv[2]
    action = sys.argv[3]
    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = digital_report_id, log_text=f'start with argument List: {str(sys.argv[1:])} ' )
except:
    t.logger( log_type=t.LOG_TYPE_ERROR, run_id = digital_report_id, log_text=f'missing argument. This was given: {str(sys.argv[1:])}' )
    exit( -1 )

# call the activity
ret = q.dpm_activity( p_digital_report_id=digital_report_id, p_status=status, p_action=action )
exit( ret )

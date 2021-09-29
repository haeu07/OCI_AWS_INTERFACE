import tools as t
import aws_message_q as q
import sys

# get call argument
t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = 12345, log_text=f'start with argument List: {str(sys.argv[1:])} ' )
action = sys.argv[1]

# call the activity
ret = q.dpm_file_activity( p_digital_report_id=111194, p_activity=action )
exit( ret )

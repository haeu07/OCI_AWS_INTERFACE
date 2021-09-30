#!/bin/sh
echo "Activating virtual Enviroment!"

export https_proxy=https://10.11.120.229:3128/
export http_proxy=http://10.11.120.229:3128/
export ftp_proxy=https://10.11.120.229:3128/

source /ISME/STARS/PreProc/VIRTUAL_ENV/bin/activate

echo "Calling the script!"

echo "Call parameter:" $1

#python3 main.py approve
#python3 main.py reject
python3 dpm_activity_main.py $1 $2 $3

exit 0


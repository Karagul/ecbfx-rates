import requests 
import datetime
import pandas as pd
import paramiko
import os 
import functools
import logging
import logging.config
import numpy as np
from subprocess import run 


edge_node = "***"
password = "********"
username = "laffey_c"
sftp_dir = "/home/laffey_c@int/ecbfx/"
hdfs_dir = "/user/laffey_c/ecbfx/"
cluster_dir = "/home/int/laffey_c/ecbfx/"
logging_dir = "/home/laffey_c@int/ecbfx/logging_ecbfx/"


class AllowAnythingPolicy(paramiko.MissingHostKeyPolicy):
	def missing_host_key(self, client, hostname, key):
				 return


#set proxy 
http_proxy  = '''http://{0}:{1}@:8080'''.format(username, password)
https_proxy = '''https://{0}:{1}@:8080'''.format(username, password)

proxyDict = { 
              "http"  : http_proxy, 
              "https" : https_proxy
            }


def node_connection(p_dest_server,p_username,p_password):
    client = paramiko.SSHClient() 
    client.set_missing_host_key_policy(AllowAnythingPolicy()) 
    client.connect(p_dest_server,username=p_username,password=p_password)
    return client

def my_callback(filename, bytes_so_far, bytes_total): 
    x = bytes_so_far 
#print 'Transfer of %r is at %d/%d bytes (%.1f%%)' % 
#(filename, bytes_so_far, bytes_total, 100. * bytes_so_far / bytes_total) 

def do_remote_cmd(p_client,p_cmd,p_success_message,p_fail_message):
    print('Executing : '+p_cmd+"\n")
    stdin, stdout, stderr = p_client.exec_command(p_cmd)
    exit_status = stdout.channel.recv_exit_status() # Blocking call
    if exit_status == 0:
        print (p_success_message)
        print(stdout)
    else:
        print(p_fail_message)
        print('Exit Status :'+ str(exit_status))
        print("Standard Error")
        print(stderr)
    return exit_status


def push_file_to_hadoop(p_host,p_username, p_password,p_hdfs_dir,p_sftp_dir,p_cluster_dir,p_file_path):
    #push to edge
    file_name = os.path.basename(p_file_path)
    table_name = os.path.split(os.path.split(p_file_path)[0])[1]
    local_file_dir = p_sftp_dir + table_name
    os.chdir(local_file_dir)
    print ("local :"+ local_file_dir)
    client = node_connection(p_host,p_username,p_password)
    sftp = client.open_sftp()
    cluster_file_dir = p_cluster_dir + table_name
    sftp.chdir(cluster_file_dir)
    print("sftp :" + cluster_file_dir)
    callback_for_filename = functools.partial(my_callback,file_name)
    sftp.put(file_name, file_name, callback = callback_for_filename)
    #now put into hdfs 
    do_remote_cmd(client,"hdfs dfs -put -f "+p_cluster_dir + "/" + table_name+ "/" + file_name +" " +p_hdfs_dir+ table_name+ "/" ,"moved to hadoop "+p_hdfs_dir, "Error pushing to hadoop")
    client.close()
    
    
def ecbfx_upload(p_startdate,p_enddate, p_proxy_dict,p_edge_node, p_username, p_password, p_hdfs_dir, p_sftp_dir, p_cluster_dir):
    
    #format start date for API url 
    last_date_recorded1 = p_startdate.strftime('%Y-%m-%d')
    #format end date for API url 
    today1 = p_enddate.strftime('%Y-%m-%d')
    #Construst API url 
    url_fx = '''https://api.exchangeratesapi.io/history?start_at={0}&end_at={1}&base=EUR'''.format(last_date_recorded1,today1)
    fx_list = requests.get(url_fx, proxies=proxyDict)
    #Convert to dataframe
    fx_df = pd.DataFrame.from_dict(fx_list.json()['rates'], orient='index')
    fx_df = fx_df.reindex(sorted(fx_df.columns), axis=1)

    #create new df with all dates - to left join fx_df to this, to account for non-trade dates 
    end1 = p_enddate - datetime.timedelta(days=1)
    start1 = start1 = fx_df.index.min()
    list_of_dates = pd.date_range(start1, end1)

    #merge the 2 dataframes. So we now have NULLs in one data frame which is missing FXrates
    fx_df_full  = pd.merge(list_of_dates.to_frame(), fx_df, 'left', right_index= True, left_index = True)

    #forward fill these dates
    fx_df_full  = fx_df_full.ffill()



    
    
    #Crete filename to save file to 
    time_filename_encode =  p_startdate.strftime('%Y%m%d')+'_'+p_enddate.strftime('%Y%m%d')
    ecbfx_file_path = p_sftp_dir +'ecbfx/ecbfx_' + time_filename_encode + '.csv'
    #write df to csv file 
    fx_df_full.to_csv(ecbfx_file_path, sep=',', encoding='utf-8', index = False )
    #Push csv file to hadoop 
    push_file_to_hadoop(p_edge_node, p_username, p_password, p_hdfs_dir, p_sftp_dir, p_cluster_dir, ecbfx_file_path)
    
    #return a list of dates to add to a log file
    print('dates input were: ')
    print(fx_df_full.index.tolist())
    return pd.DataFrame({'date': fx_df_full.index.tolist()})
    
    
    
    
#read in a log df to find last available date, the append to this.
ecbfx_logdf = pd.read_csv(sftp_dir +'logging_ecbfx/static_logging_ecbfx.csv', sep = ',', index_col = None)



#read in last recorded date from a log df
log_dates = pd.Series([pd.to_datetime(date) for date in ecbfx_logdf['date']])
last_date_recorded = max(log_dates) 
last_date_recorded1 = last_date_recorded + datetime.timedelta(days=1)


#D-1 for todays date
today = datetime.datetime.now()

#if statement to only run if there is a day present to run for.
#lets define this as an exception and catch it in a log file 

logging.config.fileConfig(logging_dir + 'ecbfxLogging.conf', defaults={'logfilename': logging_dir + 'ecbfxError.log'})
logger = logging.getLogger('ecbfxLogger')




if not last_date_recorded.strftime('%Y-%m-%d') == today.strftime('%Y-%m-%d'):
    try:
        ecbfx_logdf = ecbfx_logdf.append(ecbfx_upload(last_date_recorded1, today, proxyDict,edge_node, username, password, hdfs_dir, sftp_dir, cluster_dir), ignore_index= True)
    except Exception as e:
        logger.exception(today.strftime('%Y-%m-%d'), e)



    #create csv for log df 
    ecbfx_logdf.to_csv(sftp_dir + 'logging_ecbfx/static_logging_ecbfx.csv', sep = ',', index=False)

    #remove files that are no longer needed
    run('rm ' + sftp_dir + 'ecbfx/*', env=os.environ, shell=True)
else: 
    print('No new rates to be uploaded...........')


    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

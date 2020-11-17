import subprocess
import pandas
import numpy
import datetime
import re

ciera = set(range(8101, 8110))
grail = set(range(8110, 8147))

def fraction_ciera(x):
    return len(set(x).intersection(ciera))

def fraction_grail(x):
    return len(set(x).intersection(grail))

def extend_nodelist(x):
    nodelist_expanded = []
    for i in x:
        if '-' in i:
            start = int(i.split('-')[0])
            stop = int( i.split('-')[1])
            nodelist_expanded.extend(list(range(start,stop+1)))
        else:
           nodelist_expanded.append(int(i))

    return nodelist_expanded

def quest_total_usage_stats(start='8/01/19', stop='11/01/19'):
    # b1095 node usage
    result = subprocess.run(['sacct', '--nodelist=qnode8110,qnode8111,qnode8112,qnode8113,qnode8114,qnode8115,qnode8116,qnode8117,qnode8118,qnode8119,qnode8120,qnode8121,qnode8122,qnode8123,qnode8124,qnode8125,qnode8126,qnode8127,qnode8128,qnode8129,qnode8130,qnode8131,qnode8132,qnode8133,qnode8134,qnode8135,qnode8136,qnode8137,qnode8138,qnode8139,qnode8140,qnode8141,qnode8142,qnode8143,qnode8144,qnode8145,qnode8146', '--starttime={0}'.format(start), '--endtime={0}'.format(stop), '--format=NodeList%1000,CPUTimeRAW,Account,NNodes,ncpus,ReqMem', '--allocations'],stdout=subprocess.PIPE)
    list_of_results = [list(filter(None, i.split(" "))) for i in result.stdout.decode('utf-8').split('\n')[6::]]
    list_of_results = list_of_results[0:-1] 
    b1095_usage = pandas.DataFrame(list_of_results, columns=['NodeList', 'CPUTimeSeconds', 'Account', 'NumberOfNodes','NumberOfCpus','RequestedMemory'])

    # need to get rid of the CPUtime of general access users from part of job not run on these nodes
    b1095_usage.NumberOfNodes = b1095_usage.NumberOfNodes.astype(float)
    b1095_usage.NumberOfCpus = b1095_usage.NumberOfCpus.astype(float)
    b1095_usage.CPUTimeSeconds = b1095_usage.CPUTimeSeconds.astype(float)
    def compute_mem(x):
        if 'Mc' in x.RequestedMemory:
            return (int(x.RequestedMemory.replace("Mc",""))*0.001*x.NumberOfCpus)/x.NumberOfNodes
        elif 'Gc' in x.RequestedMemory:
            return (int(x.RequestedMemory.replace("Gc",""))*x.NumberOfCpus)/x.NumberOfNodes
        elif 'Gn' in x.RequestedMemory:
            return int(x.RequestedMemory.replace("Gn",""))
        elif 'Mn' in x.RequestedMemory:
            return int(x.RequestedMemory.replace("Mn",""))*0.001
    breakpoint()
    b1095_usage["MemoryPerNode"] = b1095_usage.apply(compute_mem, axis=1)
    jobs_possibly_not_all_on_b1095 = b1095_usage.loc[(b1095_usage.Account != 'b1095') & (b1095_usage.NumberOfNodes > 1)]
    jobs_possibly_not_all_on_b1095.NodeList = jobs_possibly_not_all_on_b1095.NodeList.apply(lambda x: x.replace('qnode', '').replace('qhimem', '').replace('qgpu', '').strip('[').strip(']').split(','))
    jobs_possibly_not_all_on_b1095.NodeList = jobs_possibly_not_all_on_b1095.NodeList.apply(extend_nodelist)

    # now we see how many of the nodes where actually CIERA nodes and then divide the CPURAW time accordingly.
    jobs_possibly_not_all_on_b1095.CPUTimeSeconds = (jobs_possibly_not_all_on_b1095.NodeList.apply(fraction_grail)/ jobs_possibly_not_all_on_b1095.NumberOfNodes) * jobs_possibly_not_all_on_b1095.CPUTimeSeconds 
    b1095_usage.loc[jobs_possibly_not_all_on_b1095.index, 'CPUTimeSeconds'] = jobs_possibly_not_all_on_b1095.CPUTimeSeconds


    # b1094 node usage
    result = subprocess.run(['sacct', '--nodelist=qnode8101,qnode8102,qnode8103,qnode8104,qnode8105,qnode8106,qnode8107,qnode8108,qnode8109', '--starttime={0}'.format(start), '--endtime={0}'.format(stop), '--format=NodeList%1000,CPUTimeRAW,Account,NNodes', '--allocations'], stdout=subprocess.PIPE)
    list_of_results = [list(filter(None, i.split(" "))) for i in result.stdout.decode('utf-8').split('\n')[6::]]
    list_of_results = list_of_results[0:-1]
    b1094_usage = pandas.DataFrame(list_of_results, columns=['NodeList', 'CPUTimeSeconds', 'Account', 'NumberOfNodes',])

    # need to get rid of the CPUtime of general access users from part of job not run on these nodes
    b1094_usage.NumberOfNodes = b1094_usage.NumberOfNodes.astype(float)
    b1094_usage.CPUTimeSeconds = b1094_usage.CPUTimeSeconds.astype(float)
    jobs_possibly_not_all_on_b1094 = b1094_usage.loc[(b1094_usage.Account != 'b1094') & (b1094_usage.NumberOfNodes > 1)]
    jobs_possibly_not_all_on_b1094.NodeList = jobs_possibly_not_all_on_b1094.NodeList.apply(lambda x: x.replace('qnode', '').replace('qhimem', '').replace('qgpu', '').strip('[').strip(']').split(','))
    jobs_possibly_not_all_on_b1094.NodeList = jobs_possibly_not_all_on_b1094.NodeList.apply(extend_nodelist)

    # now we see how many of the nodes where actually CIERA nodes and then divide the CPURAW time accordingly.
    jobs_possibly_not_all_on_b1094.CPUTimeSeconds = (jobs_possibly_not_all_on_b1094.NodeList.apply(fraction_ciera)/ jobs_possibly_not_all_on_b1094.NumberOfNodes) * jobs_possibly_not_all_on_b1094.CPUTimeSeconds
    b1094_usage.loc[jobs_possibly_not_all_on_b1094.index, 'CPUTimeSeconds'] = jobs_possibly_not_all_on_b1094.CPUTimeSeconds

    # now we can use this info to create an informative table
    # Add CPU Hours table
    b1094_usage['CPUTimeHours'] = b1094_usage.CPUTimeSeconds.apply(lambda x: x/(60*60))
    b1095_usage['CPUTimeHours'] = b1095_usage.CPUTimeSeconds.apply(lambda x: x/(60*60))


    # how many days?
    duration = datetime.datetime.strptime(stop, '%m/%d/%y')  - datetime.datetime.strptime(start, '%m/%d/%y')  
    days = duration.days

    # Make new dataframe
    percent_usage_df = pandas.DataFrame({
                                        'b1094_available_cpu_hours': len(ciera)*28*24*days,
                                        'b1094_used_cpu_hours': b1094_usage['CPUTimeHours'].sum(),
                                        'b1094_ciera_used_cpu_hours' : b1094_usage.loc[b1094_usage.Account == 'b1094']['CPUTimeHours'].sum(),
                                        'b1094_general_nu_used_cpu_hours' : b1094_usage.loc[b1094_usage.Account != 'b1094']['CPUTimeHours'].sum(),
                                        'b1095_available_cpu_hours': len(grail)*28*24*days,
                                        'b1095_used_cpu_hours': b1095_usage['CPUTimeHours'].sum(),
                                        'b1095_ciera_used_cpu_hours' : b1095_usage.loc[b1095_usage.Account == 'b1095']['CPUTimeHours'].sum(),
                                        'b1095_general_nu_used_cpu_hours' : b1095_usage.loc[b1095_usage.Account != 'b1095']['CPUTimeHours'].sum(),
                                        'time_start': start, 'time_end' : stop}, index=[0])
    
    return percent_usage_df

def quest_stats(account='b1095',start='5/01/19', stop='5/31/20'):
    # QUEST usage over the last month for that account
    result = subprocess.run(['sreport','cluster','AccountUtilizationByUser','accounts={0}'.format(account),'start={0}'.format(start),'end={0}'.format(stop),'format=Accounts,Cluster,TresCount,Login,Proper,Used'], stdout=subprocess.PIPE)
    usage_list = [list(filter(None, i.split(" "))) for i in result.stdout.decode('utf-8').split('\n')[6::]]
    usage_list = usage_list[0:-1]
    [i.pop(6) for i in usage_list if len(i) == 8]
    usage_list[0].insert(3, 'ALL')
    usage_list[0].insert(4, 'ALL')
    usage_list[0].insert(5, 'ALL')
    usage_table = pandas.DataFrame(usage_list, columns =['Account', 'Cluster', 'TRES Count', 'Login', 'First Name', 'Last Name', 'CPU'])
    # convert CPU minutes to hours
    usage_table['CPU'] = usage_table['CPU'].astype(int).apply(lambda x: x/60)

    return usage_table

def account_membership_df(account):
        # get membership info
    members_table = get_account_membership(account=account)

    # get partition associated wtih this account
    partition_table = get_partition_membership(account=account)

    members_table = members_table.merge(partition_table, on=['members'], how='outer')

    return members_table

def get_account_membership(account='b1095'):
    # Current membership of that account
    result = subprocess.run(['grep', account, '/etc/group'], stdout=subprocess.PIPE)
    # members list
    members_list = result.stdout.decode('utf-8').split(':')[-1].split('\n')[0].split(',')
    # account
    account_list = [account] *len(members_list)
    # name of user
    name_list = []
    # last log in of user
    last_login_list = []
    for member in members_list:
        result = subprocess.run(['finger', member], stdout=subprocess.PIPE)
        name_list.append(result.stdout.decode('utf-8').split('Name: ')[1].split('\n')[0])
        last_login = result.stdout.decode('utf-8')
        if 'Never logged in' in last_login:
            last_login_list.append('Never')
        elif 'On since' in last_login:
            last_login_list.append(last_login.split('On since ')[-1].split(' on')[0])
        else:
            last_login_list.append(last_login.split('Last login ')[-1].split(' on')[0])
        
    # current members table
    members_table = pandas.DataFrame(numpy.asarray([members_list, account_list, name_list, last_login_list]).T, columns=['members', 'account', 'name', 'LastLogin'])
    return members_table

def get_partition_membership(account='b1095'):
    # Current partitions and who is on them of the accounts
    result = subprocess.run(['grep', account,'/etc/slurm/slurm.conf'], stdout=subprocess.PIPE)
    partitions_list = result.stdout.decode('utf-8').split('PartitionName=')[1::] 
    all_partitions = pandas.DataFrame()
    for partition in partitions_list:
        users = partition.split('AllowGroups=')[-1].split(' ')[0].split(',')
        partition_name = [partition.split(' ')[0]] * len(users)
        all_partitions = all_partitions.append(pandas.DataFrame(numpy.asarray([users, partition_name]).T, columns=['members', 'partitions']))
    return all_partitions

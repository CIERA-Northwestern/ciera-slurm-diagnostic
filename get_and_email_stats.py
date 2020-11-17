from ciera_diagnostics import usage, quickstart
import pandas

# loop over reports for all CIERA buy-in accounts
accounts = ['b1095', 'b1094', 'b1119']

# time period for usage report
"""
time_periods = [['5/01/19', '8/01/19'],
               ['8/01/19', '11/01/19'],
               ['11/01/19', '2/01/20'],
               ['2/01/20', '5/01/20'],
               ['5/01/20', '8/01/20'],
               ]
"""

time_periods = [['8/01/20', '11/01/20']]
# user info
all_accounts_members = pandas.DataFrame()
for account in accounts:
    members_table = usage.account_membership_df(account)
    all_accounts_members = all_accounts_members.append(members_table)

all_accounts_members.to_csv(open('members_report.csv', 'w'), index=False)

# SLURM usage info
all_percent_usage_of_nodes = pandas.DataFrame()

for time_period in time_periods:
    all_accounts_usage = pandas.DataFrame()
    percent_usage_df = usage.quest_total_usage_stats(start=time_period[0], stop=time_period[1])
    all_percent_usage_of_nodes = all_percent_usage_of_nodes.append(percent_usage_df)
    for account in accounts: 
        usage_table = usage.quest_stats(account=account, start=time_period[0], stop=time_period[1])
        all_accounts_usage = all_accounts_usage.append(usage_table)


    name_of_accounts_usage_file = 'usage_report_{0}_{1}.csv'.format(time_period[0].replace('/','-'), time_period[1].replace('/','-'))
    name_of_percentage_usage_file = 'percentage_report_{0}_{1}.csv'.format(time_period[0].replace('/','-'), time_period[1].replace('/','-'))

    all_accounts_usage.to_csv(open(name_of_accounts_usage_file, 'w'), index=False)
    all_percent_usage_of_nodes.to_csv(open(name_of_percentage_usage_file, 'w'), index=False)

    quickstart.send_stats(message_text='CSV attached reports', to='mnballer1992@gmail.com', text_type='plain',
                          subject="Monthly QUEST Usage Report: {0} to {1}".format(time_period[0], time_period[1]),
                          filenames=['members_report.csv', name_of_accounts_usage_file, name_of_percentage_usage_file], credentials_token='token.pickle')





#html = """ 
#<html>
#  <head></head>
#  <body>
#    {0}
#    <br>
#    <br>
#    {1}
#    <br>
#    <br>
#  </body>
#</html>
#""".format(all_accounts_members.to_html(), all_accounts_usage.to_html())

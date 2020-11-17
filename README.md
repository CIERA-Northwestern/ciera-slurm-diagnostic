# Environment on QUEST
```
module purge all
module load python/anaconda3.6
source activate /projects/b1095/anaconda3/envs/ciera-stats-py37/
```

# Running code

Open `get_and_email_stats.py` and set time period (we usually run this every three months). For instance, change

```
time_periods = [['8/01/20', '11/01/20']]
```
to
```
time_periods = [['11/01/20', '2/01/21']]
```

then

```
python get_and_email_stats.py
```

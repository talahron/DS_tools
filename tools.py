# -*- coding: utf-8 -*-
"""
Created on Sun Dec  4 17:26:35 2022

@author: talah
"""
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# hist
plt.hist(df_hist['customer_weight'],bins=range(0, 200, 5)
         
# wgere
df['has_weight'] = np.where(df['visit_weight']>0,1,0) 

# agg
df_device_agg = df.groupby(['gym','device_id']).agg({
                            'visit_id':pd.Series.nunique,
                            'customer_weight':'mean',
                            'visit_weight':'sum',
                            'distance_from_home':'mean',
                            'distance_from_work':'mean',
                            'has_weight':'sum',
                            'visit_duration':'mean'}).reset_index()

# concat
df = pd.concat([df, gym_df], ignore_index=True)

# join
df = df.merge(df_info,how='left',on='venue_id')


# plot line
x = df_month_year_agg[df_month_year_agg['gym']==gym][['visit_month_year']]
x = [t[0].to_timestamp() for t in x.values]
y = df_month_year_agg[df_month_year_agg['gym']==gym][['average_week_visits_per_cost']]
y = [i[0] for i in y.values]

plt.plot(x, y)
plt.ylabel=(gym)
plt.show()
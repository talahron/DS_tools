def create_and_apply_scaler(x):
    #trains the scaler on the values without outliers
    ind=x.index
    n=x.name
    scaler=preprocessing.MinMaxScaler()
    #try 95th percentile
    #for_scaling_without_outliers = x[(x<=x.mean()+3*x.std()) & (x>=x.mean()-3*x.std())]
    #iq_r=iqr(x)
    #for_scaling_without_outliers = x[(x<=x.median()+1.5*iq_r) & (x>=x.median()-1.5*iq_r)]
    v=np.nanpercentile(x,99)
    print('v=',v)
    print('min x=',min(x))
    for_scaling_without_outliers=x[x<v]
    if len(for_scaling_without_outliers)<len(x):
        print('without outliers %s out of total %s' % (len(for_scaling_without_outliers),len(x)))
    if len(for_scaling_without_outliers)>0:
        scaler.fit(np.array(for_scaling_without_outliers).reshape(-1,1))
        #applies the scaler
        scaled = scaler.transform(np.array(x).reshape(-1,1))       
        x=pd.Series(scaled.reshape(1,-1)[0],index=ind,name=n)
        x[x<0]=0
        x[x>1]=1
        return x
    else:
        return pd.Series([np.nan]*len(x),index=ind,name=n)
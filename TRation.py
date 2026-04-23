df = spark.table("hive_metastore.default.ration_data")

df = df.toPandas()

df = df.drop_duplicates()
df = df.fillna(0)

df['month'] = df['month'].astype(str)

df['shortage_flag'] = df['available_stock'] < df['predicted_demand']

df['hunger_risk'] = (
    (df['predicted_demand'] - df['available_stock']) * 0.5 +
    df['complaint_count'] * 0.3 +
    df['transaction_count'] * 0.2
)
df['hunger_risk'] = df['hunger_risk'].clip(0, 100)

df['allocation'] = df['predicted_demand'] * 1.05

def decision(row):
    if row['shortage_flag']:
        return "Increase Supply"
    else:
        return "Stable"

df['ai_decision'] = df.apply(decision, axis=1)

df['crisis_alert'] = df['hunger_risk'] > 70

df['behavior_flag'] = df['transaction_count'] > df['transaction_count'].mean()

df['anomaly_flag'] = df['transaction_count'] > (df['transaction_count'].mean() * 1.5)

import matplotlib.pyplot as plt

df.groupby('district')['predicted_demand'].sum().plot(kind='bar')
plt.title("District-wise Demand")
plt.xlabel("District")
plt.ylabel("Predicted Demand")
plt.show()

shortage_data = df[df['shortage_flag'] == True]

shortage_data.groupby('district')['predicted_demand'].sum().plot(kind='bar')
plt.title("District-wise Shortage")
plt.xlabel("District")
plt.ylabel("Shortage Demand")
plt.show()

df.to_csv('/tmp/final_output.csv', index=False)

display(df)

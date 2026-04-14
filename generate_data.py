from src.data.ingestion.synthetic_data import generate_claims_data

df = generate_claims_data(10000, 0.24)
df.to_csv('data/raw/insurance_claims.csv', index=False)
print(f'Generated {len(df)} rows')
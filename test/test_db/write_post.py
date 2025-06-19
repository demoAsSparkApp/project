import os
from datetime import datetime

def write_parquet(df, output_dir: str, base_name: str):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_path = os.path.join(output_dir, f"{base_name}_{timestamp}.parquet")
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Data saved to: {output_path}")
import pandas as pd
import re

input_path = "/mnt/data/signup.csv"

# Load CSV safely
df = pd.read_csv(input_path)

original_count = len(df)

# Normalize column names
df.columns = [col.strip().lower() for col in df.columns]

# -------------------------
# 1. Standardize Date Format
# -------------------------
if "signup_date" in df.columns:
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce").dt.strftime("%Y-%m-%d")

# -------------------------
# 2. Flag Low Quality Leads
# -------------------------
def is_low_quality(row):
    patterns = ["test", "asdf", "qwerty", "12345", "dummy"]
    row_str = " ".join([str(x).lower() for x in row])
    
    # Missing critical fields
    if pd.isna(row.get("email")) or pd.isna(row.get("name")):
        return True
    
    # Garbage/test patterns
    for pattern in patterns:
        if pattern in row_str:
            return True
    
    # Invalid email format
    if not re.match(r"[^@]+@[^@]+\.[^@]+", str(row.get("email"))):
        return True
    
    return False

df["is_low_quality"] = df.apply(is_low_quality, axis=1)

quarantine_df = df[df["is_low_quality"] == True].copy()
clean_df = df[df["is_low_quality"] == False].copy()

# -------------------------
# 3. Deduplication + Multi-plan Logic
# -------------------------
if "signup_date" in clean_df.columns:
    clean_df["signup_date_dt"] = pd.to_datetime(clean_df["signup_date"], errors="coerce")
    clean_df = clean_df.sort_values(by="signup_date_dt", ascending=False)

if "email" in clean_df.columns and "plan" in clean_df.columns:
    multi_plan_counts = clean_df.groupby("email")["plan"].nunique()
    multi_plan_users = multi_plan_counts[multi_plan_counts > 1].index
    clean_df["is_multi_plan"] = clean_df["email"].isin(multi_plan_users)
    clean_df = clean_df.drop_duplicates(subset=["email"], keep="first")

clean_df = clean_df.drop(columns=["is_low_quality", "signup_date_dt"], errors="ignore")
quarantine_df = quarantine_df.drop(columns=["is_low_quality"], errors="ignore")

members_final_path = "/mnt/data/members_final.csv"
quarantine_path = "/mnt/data/quarantine.csv"

clean_df.to_csv(members_final_path, index=False)
quarantine_df.to_csv(quarantine_path, index=False)

final_count = len(clean_df)
quarantine_count = len(quarantine_df)

original_count, final_count, quarantine_count, members_final_path, quarantine_path

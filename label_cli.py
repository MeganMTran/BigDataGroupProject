import argparse
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--file", required=True)
a = ap.parse_args()

df = pd.read_csv(a.file, dtype=str).fillna("")
opts = {"1": "World", "2": "Sports", "3": "Business", "4": "Sci/Tech", "5": "None of these"}
print("1=World 2=Sports 3=Business 4=Sci/Tech 5=None of these  (q = save and quit)\n")
for i, row in df.iterrows():
    if row["my_label"].strip():
        continue
    print(f"[{i+1}/{len(df)}] {row['text'][:300]}")
    while True:
        c = input("> ").strip().lower()
        if c == "q":
            df.to_csv(a.file, index=False)
            raise SystemExit("Saved. Rerun to continue.")
        if c in opts:
            df.at[i, "my_label"] = opts[c]
            break
        print("Enter 1-5 or q")
df.to_csv(a.file, index=False)
print("All labeled and saved.")

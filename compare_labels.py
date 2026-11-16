import argparse
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--file", required=True)
a = ap.parse_args()

df = pd.read_csv(a.file, dtype=str)
df["my_label"] = df["my_label"].fillna("").str.strip()
labeled = df[df["my_label"] != ""].copy()
print(f"Rows with a label: {len(labeled)} of {len(df)}")
if labeled.empty:
    raise SystemExit("No labels found. Fill in the my_label column and save as CSV first.")

none = labeled[labeled["my_label"].str.lower() == "none of these"]
fit = labeled.drop(none.index)
ok = fit["my_label"].str.lower() == fit["model_prediction"].str.strip().str.lower()

print(f"Fit one of the four classes: {len(fit)}, none of these: {len(none)}")
print(f"Accuracy on the four-class messages: {ok.mean():.3f}")
print("\nExamples the model got wrong:")
print(fit[~ok].head(8).to_string(index=False))

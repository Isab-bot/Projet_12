import pandas as pd

df = pd.read_csv(
    "data/fakeddit/multimodal_train.tsv",
    sep="\t",
    usecols=["subreddit", "6_way_label"],
)
for label, subreddits in df.groupby("6_way_label")["subreddit"].unique().items():
    print(label, sorted(subreddits))
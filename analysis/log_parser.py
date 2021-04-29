import re
import pandas as pd
import time 
from datetime import datetime
from datetime import timedelta
from pathlib import Path

INVERT_STATUS = re.compile("(\\[\\d+:\\d+.\\d+]) Processed.*a total of ([,\\d]+)")
MERGE_STATUS = re.compile("(\\[\\d+:\\d+.\\d+]).So far merged a total of ([\\d,]+) tokens out of ([\\d,]+)")
MADE_INDEX = re.compile("(\\[\\d+:\\d+.\\d+]) Finished creating temporary index at (.*)")
TIME_FORMAT = "[%H:%M.%S]"

NUM_REVIEWS = re.compile("^getNumberOfReviews: (\\d+)")


def parse_log(lines):
    first_time = None
    entries = []
    index_entries = []
    total_tokens = 0
    total_indices_size = 0
    total_num_reviews = 0
    for line in lines:
        invert = INVERT_STATUS.match(line)
        if invert:
            ttime = invert.group(1)
            tokens = int(invert.group(2).replace(',', ''))
            ttime = datetime.strptime(ttime, TIME_FORMAT)
            if not first_time:
                first_time = ttime
            delta = ttime - first_time
            entries.append(("invert", delta, tokens))
            continue
        index = MADE_INDEX.match(line)
        if index:
            ttime = index.group(1)
            ttime = datetime.strptime(ttime, TIME_FORMAT)
            delta = ttime - first_time
            # index_path = index.group(2).replace("all", "all_good_seq_parser")
            index_path = index.group(2)
            index_path = Path(index_path)
            cur_index_size = sum(f.stat().st_size for f in index_path.glob('**/*') if f.is_file())
            total_indices_size += cur_index_size

            index_entries.append((delta, cur_index_size, total_indices_size))
        merge = MERGE_STATUS.match(line)
        if merge:
            ttime = merge.group(1)
            processed = int(merge.group(2).replace(',', ''))
            line_total = int(merge.group(3).replace(',', ''))
            if not total_tokens:
                total_tokens = line_total
            assert total_tokens == line_total
            ttime = datetime.strptime(ttime, TIME_FORMAT)
            delta = ttime - first_time
            entries.append(("merge", delta, processed))
        n_reviews = NUM_REVIEWS.match(line)
        if n_reviews:
            assert total_num_reviews == 0
            total_num_reviews = n_reviews.group(1)

    assert total_num_reviews != 0
    df1 = pd.DataFrame.from_records(entries, columns=["stage", "time", "numTokens"])
    df2 = pd.DataFrame.from_records(index_entries, columns=["time", "index_size", "total_index_size"])
    df1["total_reviews"] = total_num_reviews 
    df1["total_reviews"] = df1["total_reviews"].astype("int")
    df2["total_reviews"] = total_num_reviews
    df2["total_reviews"] = df2["total_reviews"].astype("int")
    return df1, df2

def parse_log_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as log:
        return parse_log(log)

if __name__ == "__main__":
    df = parse_log_file("E:\\webdata_datasets\\all_good_seq_parser\\log-sequential-regex.txt")
    print(df)

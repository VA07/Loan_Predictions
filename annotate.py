import os
import settings
import pandas as pd

def read():
    acquisition = pd.read_csv(os.path.join(settings.PROCESSED_DIR, "Acquisition.txt"), sep="|")
    return acquisition

def count_performance_rows():
    """ Counts the number of rows in performance data associated with
	    a particular loan id """
    counts = {}
    with open(os.path.join(settings.PROCESSED_DIR, "Performance.txt"), 'r') as f:
        for i, line in enumerate(f):
		    # Skip header row
            if i == 0:
                continue
            loan_id, date = line.split("|")
            loan_id = int(loan_id)
            if loan_id not in counts:
                counts[loan_id] = {
                    "foreclosure_status": False,
                    "performance_count": 0
                }
            counts[loan_id]["performance_count"] += 1
            if len(date.strip()) > 0:
                counts[loan_id]["foreclosure_status"] = True
    return counts

def get_performance_summary_value(loan_id, key, performance_summary):
    """Get performance summary of the data associated with loan id"""
    value = performance_summary.get(loan_id, {
        "foreclosure_status": False,
        "performance_count": 0
    })
    return value[key]

def annotate(acquisition, performance_summary):
    """converts all columns into numeric values suitable for ml algo
	   dates are split into two columns month and year. strings into
	   category variables. Fills any missing values with -1.     """
    acquisition["foreclosure_status"] = acquisition["id"].apply(lambda x: get_performance_summary_value(x, "foreclosure_status", performance_summary))
    acquisition["performance_count"] = acquisition["id"].apply(lambda x: get_performance_summary_value(x, "performance_count", performance_summary))
    for column in [
        "channel",
        "seller",
        "first_time_homebuyer",
        "loan_purpose",
        "property_type",
        "occupancy_status",
        "property_state",
		"relocation_indicator"
    ]:
        acquisition[column] = acquisition[column].astype('category').cat.codes

    for start in ["first_payment", "origination"]:
        column = "{}_date".format(start)
        acquisition["{}_year".format(start)] = pd.to_numeric(acquisition[column].str.split('/').str.get(1))
        acquisition["{}_month".format(start)] = pd.to_numeric(acquisition[column].str.split('/').str.get(0))
        del acquisition[column]

    acquisition = acquisition.fillna(-1)
	#discard values with less count
    acquisition = acquisition[acquisition["performance_count"] > settings.MINIMUM_TRACKING_QUARTERS]
    return acquisition

def write(acquisition):
    """Write the csv values to train.csv file"""
    acquisition.to_csv(os.path.join(settings.PROCESSED_DIR, "train.csv"), index=False)

if __name__ == "__main__":
    """main function"""
    acquisition = read()
    performance_summary = count_performance_rows()
    acquisition = annotate(acquisition, performance_summary)
    write(acquisition)
## DATA PIPELINES

The focus of these data pipelines is to create active utilities that can sync new data automatically. The data is stored locally and used for model development and testing throughout the production process.

Key ISOs:
**ERCOT**, **PJM**, **CAISO**

The most publically available data is the ERCOT ISO:
* 2-Day Real Time Gen and Load Data Reports: Reports include generation summaries by disclosure area in independent excel sheets. Key generation resources: NON-IRR (Natural Gas, Coal, Oil), PVGR (Photo-Voltaic), WGR (Wind Generation Resource), ESR (Energy Storage) + Charging + Discharging, TELEM (Telemetry reading for total output). Daily updates.
* 2-Day SCED Energy Curves: Aggregate energy demand curves for CLR (Controllable load resources)

### Methodology

Using GridStatus open-source ERCOT API client, I developed a pipeline method to pull historical supply and demand curves for all or individual resources in Houston.
1. Reuse GridStatus authentication and request-handling code to manage bearer tokens and subscription key when formatting request to ERCOT API.
2. Add a custom extraction layer for the Houston-specific **NP_908-ER 2-Day SCED energy curves.** to pull aggregate supply and demand curves
2. Paginate through the results and save the original records without fitting or flattening the curves into a quadratic model.

### Run Process ###

```
import pandas as pd
from ercot_api import run_pipeline, fetch_energy_curve

import gridstatus

# Dates and settings for this particular request
START_DATE = "2026-09-05T00:00:00"
END_DATE = "2026-09-06T23:59:59"

OUTPUT_DIR = "../data/raw/np3_908_er/houston/.csv"
```



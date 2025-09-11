# deweypy

## Dewey Data Python Client

### Getting Started

To download your data using the Dewey Data Python Client, follow these steps in your command line or terminal:
1. **Install the client**
`pip install https://github.com/Dewey-Data/deweypy.git`
2. **Make a directory for your downloads**
3. **Locate `FOLDER_ID`**
`FOLDER_ID` can be extracted from the end of the end of the API URL after `data/`
`mkdir dewey-downloads/`
4. **Run client**
`-m deweypy --api-key <YOUR_API_KEY> speedy-download <FOLDER_ID>`


#### A few notes...
- For now, please use the CLI to download data. This method is well tested; notebook support will be available soon.
- Increasing the number of workers for multi-threaded downloads yields diminishing returns, as API requests are limited both by our bucket’s rate limits and your own. We recommend the default of 8 workers, but you can override this with:
`--num-workers <INT>` following `speedy-download <FOLDER_ID>`.

### Working with data post-download
For guidance on analyzing your downloaded data, check out the [provided notebook tutorial](https://github.com/Dewey-Data/deweypy/blob/main/notebook-examples/customized-monthly-patterns.ipynb). It demonstrates how to work with Polars, Pandas, and DuckDB, and includes methods for exporting data to Parquet format for more efficient downstream analysis.

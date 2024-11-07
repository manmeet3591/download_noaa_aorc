import xarray as xr
import fsspec
import numpy as np
import s3fs
import zarr
from tqdm import tqdm
from dask.distributed import Client

def main():
    base_url = 's3://noaa-nws-aorc-v1-1-1km'
    start_date = "2019-03-28"  # Specify the start date here (YYYY-MM-DD)
    end_date = "2019-12-31"  # Optional: specify an end date if desired (e.g., "2022-12-31")

    client = Client()

    for year_ in tqdm(range(2019, 2024)):
        year = str(year_)
        print(f"Processing year: {year}")
        
        # Define the dataset URL for the current year
        single_year_url = f'{base_url}/{year}.zarr/'
        
        # Load the dataset for the specific year
        ds_single = xr.open_zarr(fsspec.get_mapper(single_year_url, anon=True), consolidated=True)
        
        # Filter dataset by date, starting from the specified start date
        if start_date or end_date:
            ds_single = ds_single.sel(time=slice(start_date, end_date))
        
        # Ensure time is sorted to make sure resampling aligns properly
        ds_single = ds_single.sortby("time")

        # Resample the dataset into 24-hour chunks (1 day)
        daily_groups = ds_single.resample(time="24H")

        # Process each 24-hour chunk in the dataset
        for i, (day, daily_data) in enumerate(daily_groups):
            # Convert the day (numpy.datetime64) to a string in the format "YYYYMMDD"
            date_str = np.datetime_as_string(day, unit='D').replace('-', '')
            
            # Create a filename for each chunk (24-hour data)
            filename = f'noaa_aorc_usa_{year}_day_{date_str}.nc'
            
            # Save daily data to NetCDF file
            daily_data.to_netcdf(filename)
            print(f"Saved {filename}")

if __name__ == "__main__":
    main()

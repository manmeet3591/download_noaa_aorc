import xarray as xr
import numpy as np

# Specify the start date and, optionally, the end date
start_date = "2018-04-16"  # Format: YYYY-MM-DD
end_date = None  # Optional: e.g., "2018-12-31"

# Load dataset
filename_ = 'noaa_aorc_usa_2018.nc'
ds = xr.open_dataset(filename_, chunks={'time': 24, 'latitude': 500, 'longitude': 500})

# Filter dataset by date range
if start_date or end_date:
    ds = ds.sel(time=slice(start_date, end_date))

# Resample the dataset into 24-hour chunks (1 day)
grouped = ds.resample(time="24H")

# Process each 24-hour chunk in the dataset
for i, (day, daily_data) in enumerate(grouped):
    # Convert the day (numpy.datetime64) to a string in the format "YYYYMMDD"
    date_str = np.datetime_as_string(day, unit='D').replace('-', '')
    
    # Create a filename for each chunk (24-hour data)
    filename = f'noaa_aorc_usa_2018_day_{date_str}.nc'
    
    # Save daily data to NetCDF file
    daily_data.to_netcdf(filename)
    print(f"Saved {filename}")

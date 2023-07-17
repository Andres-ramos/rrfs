import xarray as xr
from .S3 import s3
from .Cache import cache
from shapely import Point

#Receive variable from somewhere else
cache_name="store"
bucket = "noaa-rrfs-pds"

#Class that manages rrfs model output files
class Rrfs:

    def __init__(self):
        self.cache = cache.cache(cache_name)
        self.s3_connection = s3.s3(bucket)

    #Fetches model outpust for given initialization time and forecast hour list
    #Should be the only method exposed by rrfs
    #Retuns xarray list

    #Input: 
    # date_time : pd Timestamp object, 
    # initialization_date : int, 
    # forecast_hours : [int] | int 
    #Optional parameters 
    # bounding box : shapely polygon object
    # variable list : [string] 

    #Output: 
    # xr : [xarray dataset] 
    def fetch_model_outputs(self, initialization_date, forecast_hours, bounding_box=False, variable_list=False):
        
        #TODO: Add input validation!!!

        #Stores forecast dataframes in a list
        forecasts = []

        #If the only one forecast hour was requested
        if type(forecast_hours) == int:
            forecast_hour = forecast_hours
            return self.fetch_model_output(initialization_date, forecast_hour, bounding_box, variable_list)

        #If the forecasts hours are a list
        elif type(forecast_hours) == list:
            #Opens each forecast and appends to list
            for hour in forecast_hours:
                forecasts.append(self.fetch_model_output(initialization_date, hour, bounding_box, variable_list))
            
            #Makes the list into a data frame with a time dimension
            # xr = self.make_data_frame(forecasts)
            return forecasts

        else :
            raise Exception(f'{type(forecast_hours)} as forecast hours is not supported')
        

    #Fetches model output for given initialization time and forecast hour
    #Returns xarray dataset

    #Input: 
    # date_time : pd Timestamp object, 
    # initialization_date : int, 
    # forecast_hour : int 
    #Optional parameters 
    # bounding box : shapely polygon object
    # variable list : [string] 

    #Output: 
    # xr : xarray dataset 
    def fetch_model_output(self, initialization_date, forecast_hour, bounding_box=False, variable_list=False):

        init_hour_str = initialization_date.strftime("%H")      #S3 init hour
        init_date_str = initialization_date.strftime("%Y%m%d")  #S3 init_date 

        file_name = self.make_model_file_name(init_hour_str,forecast_hour)

        #Checks cache for file
        if self.cache.check_cache(file_name, init_date_str, init_hour_str):
            #Returns file if in cache
            ds = self.cache.fetch(file_name, init_date_str, init_hour_str)

        
        #Otherwise downloads file from bucket and writes to cache
        else :
            # Path and file name for the cache level
            download_path = self.cache.get_download_path()
            # Cache file name
            cfile_name = self.cache.get_cfile_name(file_name, init_date_str, init_hour_str)
            # S3 bucket file name
            try :
                object_name = self.make_s3_object_name(file_name, init_date_str, init_hour_str)
                #Downloads file from bucket and writes it to the download path with c_file_name as filename
                self.s3_connection.download_file(object_name, download_path, cfile_name)
                #Returns cached data as xarray dataset
                ds = self.cache.fetch(file_name, init_date_str, init_hour_str)

            except: 
                raise Exception(f'Failed to download file {file_name} from bucket {bucket}')

        #If variable list was not specified
        if not variable_list:
            pass 
        
        #If variable list was specified, filter variables
        else :
            ds = self.filter_variables(ds, variable_list)
            pass 

        #If no bounding box was specified, return dataset
        if not bounding_box:
            return ds

        #If bounding box was passed, filter spatially
        else :
            return self.filter_spatially(ds, bounding_box)

    ##################### Helper functions ##############################

    #Creates the corresponding rrfs file name 
    #Follows the convention of the bucket

    #Input: 
    # initialization_date : pd timestamp object - needs year, month, day and hour
    # forecast hour: int

    #Output:
    # file_name : string 
    def make_model_file_name(self,initialization_hour, forecast_hour, output_type="nat"):
        f_hour = str(forecast_hour) if forecast_hour >= 10 else f'0{forecast_hour}'
        file_name = f'rrfs.t{initialization_hour}z.{output_type}lev.f0{f_hour}.conus_3km.grib2'
        return file_name
    
    
    #Creates object name (path in bucket) for rrfs file in bucket
    #Requires the actual file name, the date_time_Str and initialization hour

    #Input: 
    # file_name : pd timestamp object - needs year, month, day and hour
    # date_time: int

    #Output:
    # file_name : string 
    def make_s3_object_name(self, file_name, date_time_str, init_hour_str):
        date_time = date_time_str.split("-")
        date_time = ''.join(map(str, date_time))
        return f"rrfs_a/rrfs_a.{date_time}/{init_hour_str}/control/{file_name}"


    #Creates new dataset that only has the data variables that are in the list
    
    #Input :
    # ds : xarray dataset
    # variable_list : [string]

    #Output:
    # ds : xarray dataset 
    def filter_variables(self, ds, variable_list):
        coords = dict(
                    gridlat_0=(["ygrid_0", "xgrid_0"], ds.coords['gridlat_0'].data),
                    gridlon_0=(["ygrid_0", "xgrid_0"], ds.coords['gridlon_0'].data)
                )
        data_vars = {}
        for weather_var in variable_list:
            if weather_var == "MXUPHL_P8_2L103_GLC0_max1h":
                data_vars["MXUPHL03_P8_2L103_GLC0_max1h"] = (["ygrid_0", "xgrid_0"], ds[weather_var][0].data)
                data_vars["MXUPHL25_P8_2L103_GLC0_max1h"] = (["ygrid_0", "xgrid_0"], ds[weather_var][1].data)
            else :
                data_vars[weather_var] = (["ygrid_0", "xgrid_0"], ds[weather_var].data)
        
        ds = xr.Dataset(data_vars=data_vars, coords=coords)
        return ds 
        #Filters the dataset to only contain grid points inside the bounding box

    #Input:
    # ds : xarray dataset
    # bounding_box : shapely polygon object

    #Output: 
    # ds: xarray dataset 
    def filter_spatially(self, ds, bounding_box):

        lat_grid, lon_grid = ds['gridlat_0'].values, ds['gridlon_0'].values
        lat_size = len(lat_grid) #1059
        lon_size = len(lat_grid[0]) #1799
        
        lat_indexes, lon_indexes = [], []

        #TODO: This can be made more efficient
        # Potentially, can be a hashmap and stored in memory when object is initialized

        #Finds indexes that are contained inside bounding box
        for lat_index in range(lat_size):
            for lon_index in range(lon_size):
                p = Point(lon_grid[lat_index][lon_index],lat_grid[lat_index][lon_index])
                if bounding_box.contains(p):
                    lat_indexes.append(lat_index)
                    lon_indexes.append(lon_index)
        
        #Gets min and max of lat/lon
        min_lat_index, max_lat_index = min(lat_indexes), max(lat_indexes)
        min_lon_index, max_lon_index = min(lon_indexes), max(lon_indexes)

        #Returns filtered dataset
        return ds.isel(ygrid_0=range(min_lat_index,max_lat_index), xgrid_0=range(min_lon_index,max_lon_index))
    
    
    #Prototype function
    #The idea is to concat many model outputs along the time dimension
    #not enough memory
    def make_dataframe(self,forecasts):
        df = xr.concat(forecasts, "time")
        return df 
    
    def download_outputs(self,initialization_date, forecast_hour):
        init_hour_str = initialization_date.strftime("%H")      #S3 init hour
        init_date_str = initialization_date.strftime("%Y%m%d")  #S3 init_date 

        file_name = self.make_model_file_name(init_hour_str,forecast_hour)
        # Path and file name for the cache level
        download_path = self.cache.get_download_path()
        # Cache file name
        cfile_name = self.cache.get_cfile_name(file_name, init_date_str, init_hour_str)
        # S3 bucket file name
        try :
            object_name = self.make_s3_object_name(file_name, init_date_str, init_hour_str)
            #Downloads file from bucket and writes it to the download path with c_file_name as filename
            self.s3_connection.download_file(object_name, download_path, cfile_name)
            #Returns cached data as xarray dataset
            # ds = self.cache.fetch(file_name, init_date_str, init_hour_str)

        except: 
            print(f'Failed to download file {file_name} from bucket {bucket}')
        return 
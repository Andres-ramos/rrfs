import xarray as xr
from .S3 import s3
from .Cache import cache
from shapely import Point

cache_name="hrrr_store"
bucket = "noaa-hrrr-bdp-pds"

class Hrrr:

    def __init__(self):
        self.cache = cache.cache(cache_name)
        self.s3_connection = s3.s3(bucket)

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
        
    
    #Output:
    # file_name : string 
    def make_model_file_name(self,initialization_hour, forecast_hour, output_type="nat"):
        #TODO: Update this. What are the output types that are comparable at least? 
        f_hour = str(forecast_hour) if forecast_hour >= 10 else f'0{forecast_hour}'
        print(f_hour)
        file_name = f'hrrr.t{initialization_hour}z.wrfnatf{f_hour}.grib2'
        return file_name
    

    def make_s3_object_name(self, file_name, date_time_str, init_hour_str):
        date_time = date_time_str.split("-")
        date_time = ''.join(map(str, date_time))
        return f"hrrr.{date_time}/conus/{file_name}"
    

    def filter_variables(self, ds, variable_list):
        coords = dict(
                    gridlat_0=(["ygrid_0", "xgrid_0"], ds.coords['gridlat_0'].data),
                    gridlon_0=(["ygrid_0", "xgrid_0"], ds.coords['gridlon_0'].data)
                )
        data_vars = {}
        for weather_var in variable_list:
            if weather_var == "MXUPHL_P8_2L103_GLC0_max1h":
                data_vars["MXUPHL03_P8_2L103_GLC0_max1h"] = (["ygrid_0", "xgrid_0"], ds[weather_var][1].data)
                data_vars["MXUPHL25_P8_2L103_GLC0_max1h"] = (["ygrid_0", "xgrid_0"], ds[weather_var][2].data)
            elif weather_var == "VGRD_P0_L103_GLC0":
                data_vars["VGRD_P0_L103_GLC0"] = (["ygrid_0", "xgrid_0"], ds[weather_var][0].data)

            elif weather_var == "UGRD_P0_L103_GLC0":
                data_vars["UGRD_P0_L103_GLC0"] = (["ygrid_0", "xgrid_0"], ds[weather_var][0].data)
            else :
                data_vars[weather_var] = (["ygrid_0", "xgrid_0"], ds[weather_var].data)
            
            # if weather_var == "UGRD_P0_L103_GLC0"
            #     data_vars["MXUPHL03_P8_2L103_GLC0_max1h"] = (["ygrid_0", "xgrid_0"], ds[weather_var][0].data)
        
        ds = xr.Dataset(data_vars=data_vars, coords=coords)
        return ds 


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
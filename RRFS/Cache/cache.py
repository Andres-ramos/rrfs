import os
import xarray as xr 

class cache:
    def __init__(self, cache_name):
        self.cache_name = cache_name
        self.evaluate_cache(cache_name)
        return 
    
    #Evaluates if cache exists. Otherwise it creates it
    #In the future, can return memory usage information object
    def evaluate_cache(self, cache_name):

        if os.path.exists(f"{os.getcwd()}/RRFS/{cache_name}"):
            #TODO: Do stuff with the cache folder
            #      Analytics maybe on memory usage maybe?
            return 
        else :
            #Creates cache
            self.create_cache(cache_name)
            return 

    #Creates cache filesystem structure
    def create_cache(self, cache_name):
        #TODO: Generate all the dates
        dates_array = ['2023-05-24', '2023-05-23', '2023-05-25']
        #TODO: Put in an import 
        init_hour_array = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
                           '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21',
                           '22', '23']
        
        #TODO: Figure out how to programatically create path
        try :
            #Creates parent folder
            os.mkdir(f'{os.getcwd()}/RRFS/{cache_name}')
            #Creates folder for every date in the date_array
            for date in dates_array:
                os.mkdir(f'{os.getcwd()}/RRFS/{cache_name}/{date}')
                #Creates folder for every initialization hour in each date folder
                for init_hour in init_hour_array:
                    os.mkdir(f'{os.getcwd()}/RRFS/{cache_name}/{date}/{init_hour}')
            print('cache created')
        except :
            raise Exception(f"Failed to create cache")
        
        return 
    
    def get_path(self,date_time_str, init_hour_str, file_name):
        return f"{os.getcwd()}/RRFS/{self.cache_name}/{date_time_str}/{init_hour_str}"

    def check_cache(self,date_time_str, init_hour_str, file_name):
        #TODO: Figure out how to programatically create path
        if file_name in os.listdir(self.get_path(date_time_str, init_hour_str, file_name)):
            return True 
        else :
            return False
    
    def fetch(self, date_time_str, init_hour_str, file_name):
        #TODO: Figure out how to programatically create path
        return xr.open_dataset(f"{os.getcwd()}/RRFS/{self.cache_name}/{date_time_str}/{init_hour_str}/{file_name}", engine="pynio")


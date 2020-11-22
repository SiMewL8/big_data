from mrjob.job import MRJob 


# class is object contructor, like a blueprint
# while mostly everything in python is a object
# creating class called "Bacon_count" which should take properties from the MRJob (mapreduce) class

class Bacon_count(MRJob):

    # mapper function takes those as parameters and assign the input to key-value pair
    # "_" allows methods to be mapped together, so this parameter is empty

    # this function loops through each word in a line of text
    def mapper(self, _, line):
    
        #sentences are split up
        for word in line.split():
            
            #each time bacon occurs, mrjobs returns bacon, 1
            if word.lower() == "bacon":
                
                #yield returns an generotor object, not stored in memory, and process is suspended
                yield "bacon", 1
                
    # self: calls the class, key: key in kvp, values: list of values in mapper function
    # return should be key and sum of all values
    def reducer(self, key, values):
        
        yield key, sum(values)
        
#conventional python code for running program    
if __name__ == "__main__":
        
        Bacon_count.run()
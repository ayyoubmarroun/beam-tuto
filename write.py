import apache_beam

class WriteToCSV(apache_beam.DoFn):

    def process(self, element, *args, **kwargs):
        
        result = ','.join(element)
        return [result] 

def WriteToFile(filename, value):
    with open(filename, 'wa') as f:
        f.write(value)
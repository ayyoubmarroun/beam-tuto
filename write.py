import apache_beam

class FormatToCSV(apache_beam.DoFn):

    def process(self, element, *args, **kwargs):
        
        result = objectToStr(element)
        return [result] 


def objectToStr(object_):
    if isinstance(object_, (list,tuple)):
        return ','.join(map(objectToStr,object_))
    return str(object_)

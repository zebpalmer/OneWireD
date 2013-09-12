from ConfigParser import SafeConfigParser


class Settings(object):
    def __init__(self, cfgfilepath):
        try:
            settings = SafeConfigParser()
            settings.read(cfgfilepath)
        except Exception as e:
            raise Exception(e)

        try:
            for section in settings.sections():
                temp = {}
                for item in settings.items(section):
                    lines = [x.strip() for x in item[1].split(',')]
                    if len(lines) > 1:
                        temp[item[0]] = lines
                    else:
                        if item[1].lower() == 'true':
                            temp[item[0]] = True
                        elif item[1].lower() == 'false':
                            temp[item[0]] = False
                        else:
                            temp[item[0]] = item[1]

                self.__dict__[section] = temp
        except Exception:
            raise Exception('Error in config file')

    def __getattr__(self, name):
        #logging.warn("Config Section Not Found (note: inteligent IDE's will commonly/randomly trigger this)")
        return {}

"""Parameter handler to handle prepared parameters, vary parameters and fixed parameters."""
class ParaHandler():
    def __init__(self, analyticsTools, datasets):
        self.analyticsTools = analyticsTools
        self.datasets = datasets

    def handle(self,name,value,type, mtName):
        """Convert parameters to linux argument format so that template executor can process.
    
        If the parameter is dataset, all the parameters under dataset will return.
        """

        if "pre" == type :
            return self.__preHandler(name, value, mtName)
        elif "var" == type:
            return self.__varHandler(name, value, mtName)
        elif "para" == type:
            return self.__paraHandler(name, value, mtName)

    def __preHandler(self, name, value, mtName):
        subPara = []
        if "analyticstool" == name:
            subPara.append("--"  + name + " " + value )
#            subPara.append("--p " + self.analyticsTools[name]["kind"])
        elif "dataset" == name:
            subPara.append(self.datasets[value][mtName])
        else :
            subPara.append("--"  + name + " " + value )
        return subPara


    def __varHandler(self, name, value, mtName):
        val = value
        if "analyticstool" == name:
            val
#            val = val + " --p " + self.analyticsTools[value]["kind"]
        elif "dataset" == name:
            val = self.datasets[value][mtName]
        return val

    def __paraHandler(self, name, value, mtName):
        subPara = []

        if "analyticstool" == name:
            subPara.append("--"  + name + " " + value )
#            subPara.append("--p " + self.analyticsTools[value]["kind"])
        elif "dataset" == name:
            subPara.append(self.datasets[value][mtName])
        else :
            subPara.append("--"  + name + " " + value )

        return subPara

    def __printHash(self,hashTb):
        for name,value in hashTb.items():
            print name , ": " , value

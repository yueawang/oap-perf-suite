import collections
import os
import sys

# parse result file and return as {suite: {case: {config: value} } }
def parseFile(fileName):
    file = open(fileName)
    suitesLines = []
    suitesNames = []
    totalLines = 0
    res = {}
    for i, line in enumerate(file.readlines()):
        totalLines += 1
        if(line.startswith("#")):
            suitesLines.append(i + 1)
            suitesNames.append(line)
    suitesLines.append(totalLines)
    file.seek(0)
    lines = file.readlines()
    for i in range(len(suitesNames)):
        tpres = {}
        suiteStart = suitesLines[i]
        while(suiteStart < suitesLines[i + 1] - 1):
            suiteStart += 2
            caseName = lines[suiteStart - 1].split("|")[1].lstrip()
            suiteStart += 2
            tpres[caseName] = {}
            while(lines[suiteStart - 1].startswith("|")):
                median = int(lines[suiteStart - 1].split("|")[-2].lstrip())
                configName = lines[suiteStart - 1].split("|")[1].lstrip()
                suiteStart += 1
                tpres[caseName][configName] = median
        res[suitesNames[i]] = tpres
    return res

if __name__ == '__main__':
    args = sys.argv
    if(len(args) < 4):
        exit(1)
    resOldList = [parseFile(args[i]) for i in range(1, len(args) - 2)]
    resNew = parseFile(args[-2])
    baseNameList = [os.path.basename(args[i])[8:] for i in range(1, len(args) - 1)]
    htmlContent = """
    <!DOCTYPE html>
    <html>
    <style type="text/css">
        .tg {{border-collapse:collapse;border-spacing:0;}}
        .tg td {{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;}}
        .tg th {{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;}}
        .tg .tg-y2tu {{font-weight:bold;text-decoration:underline;vertical-align:top}}
        .tg .tg-baqh {{text-align:center;vertical-align:top}}
        .tg .tg-lqy6 {{text-align:right;vertical-align:top}}
        .tg .tg-yw4l {{vertical-align:top}}
        .tg .tg-ahyg {{font-weight:bold;background-color:#fe0000;vertical-align:top}}
    </style>
    <head>
	    <title>Daily Benchmark Test Result</title>
	    <meta charset="utf-8">
    </head>
    <body>
        <p>Result paths are:<br> (1){} <br> (2){}</p>
        {}
    </body>
    </html>
    """
    htmlTables = ""
    cellFormat = """<td class="tg-yw4l">{}</td>\n"""
    colorFormat = """<td class="tg-ahyg">Yes</td>\n"""
    for suite, suiteRes in resNew.items():
        suiteTable = """<table class="tg">\n"""
        suiteTable += """<tr>\n<th class="tg-baqh">{}</th>\n<th class="tg-yw4l">Config</th>\n""".format(suite)
        for baseName in baseNameList:
            suiteTable += """<th class="tg-yw41">Result of {}/ms</th>\n""".format(baseName)
        suiteTable += """<th class="tg-yw4l">Regression</th></tr>\n"""
        for case, caseRes in suiteRes.items():
            configNums = len(caseRes)
            j = 0
            odRes = collections.OrderedDict(sorted(caseRes.items()))
            for config, median in odRes.items():
                if(j == 0):
                    suiteTable += """<tr>\n<td class="tg-yw4l" rowspan="{}">{}</td>\n""".format(configNums, case)
                    suiteTable +=  """<td class="tg-yw4l">{}</td>\n""".format(config)
                else:
                    suiteTable += """<tr>\n<td class="tg-yw4l">{}</td>\n""".format(config)
                # currently we just compare with last day and present the others as history data for better judgement
                for res in resOldList:
                    if(suite in res and case in res[suite] and config in res[suite][case]):
                        suiteTable += cellFormat.format(res[suite][case][config])
                    else:
                        suiteTable += cellFormat.format("N/A")
                resLastday = resOldList[-1]
                suiteTable += cellFormat.format(median)
                if(suite in resLastday and case in resLastday[suite] and config in resLastday[suite][case]):
                    if(median > resLastday[suite][case][config]):
                        if(resLastday[suite][case][config] != 0):
                            per = (median - resLastday[suite][case][config]) * 1.0 / resLastday[suite][case][config]
                            if(per <= 0.15):
                                suiteTable += cellFormat.format("No")
                            else:
                                suiteTable += colorFormat
                        else:
                            suiteTable += colorFormat
                    else:
                        suiteTable += cellFormat.format("No")
                else:
                    suiteTable += cellFormat.format("N/A")
                suiteTable += "</tr>\n"
                j += 1
        suiteTable += "</table>\n"
        htmlTables += "\n{}\n".format(suiteTable)
    cmpFile = open(args[-1], mode='w')
    cmpFile.write(htmlContent.format("sr530:" + os.path.abspath(args[-3]), "sr530:" + os.path.abspath(args[-2]), htmlTables))
    cmpFile.close()

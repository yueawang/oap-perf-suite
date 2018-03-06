import sys
import os
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

style="""<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;}
.tg th{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;}
.tg .tg-y2tu{font-weight:bold;text-decoration:underline;vertical-align:top}
.tg .tg-baqh{text-align:center;vertical-align:top}
.tg .tg-lqy6{text-align:right;vertical-align:top}
.tg .tg-yw4l{vertical-align:top}
.tg .tg-ahyg{font-weight:bold;background-color:#fe0000;vertical-align:top}
</style>"""

if __name__ == '__main__':
    args = sys.argv
    if(len(args) != 4):
        exit(1)
    resOld = parseFile(args[1])
    resNew = parseFile(args[2])
    htmlContent = """
    <!DOCTYPE html>
    <html>
    <style type="text/css">
        .tg  {border-collapse:collapse;border-spacing:0;}
        .tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;}
        .tg th{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;}
        .tg .tg-y2tu{font-weight:bold;text-decoration:underline;vertical-align:top}
        .tg .tg-baqh{text-align:center;vertical-align:top}
        .tg .tg-lqy6{text-align:right;vertical-align:top}
        .tg .tg-yw4l{vertical-align:top}
        .tg .tg-ahyg{font-weight:bold;background-color:#fe0000;vertical-align:top}
    </style>
    <head>
	    <title>Daily Benchmark Test Result</title>
	    <meta charset="utf-8">
    </head>
    <body>
    """
    htmlTables = ""
    for suite, suiteRes in resNew.items():
        suiteTable = ""
        if(suite in resOld):
            suiteTable += """<table class="tg">\n"""
            suiteTable += """<tr>\n<th class="tg-baqh">{}</th>\n<th class="tg-yw4l">Config</th>
                                <th class="tg-lqy6">Result of {}/ms</th>
                                <th class="tg-yw4l">Result of {}/ms</th>
                                <th class="tg-yw4l">Regression</th>
                             </tr>\n""".format(suite, os.path.basename(args[1])[8:], os.path.basename(args[2])[8:])
            for case, caseRes in suiteRes.items():
                configNums = len(caseRes)
                j = 0
                for config, median in caseRes.items():
                    if(j == 0):
                        suiteTable += """<tr>\n<td class="tg-yw4l" rowspan="{}">{}</td>\n""".format(configNums, case)
                        suiteTable +=  """<td class="tg-yw4l">{}</td>\n""".format(config)
                    else:
                        suiteTable += """<tr>\n<td class="tg-yw4l">{}</td>\n""".format(config)
                    if(case in resOld[suite] and config in resOld[suite][case]):
                        suiteTable += """<td class="tg-lqy6">{}</td>\n""".format(resOld[suite][case][config])
                        suiteTable += """<td class="tg-yw4l">{}</td>\n""".format(median)
                        if(median > resOld[suite][case][config]):
                            suiteTable += """<td class="tg-ahyg">True</td>\n"""
                        else:
                            suiteTable += """<td class="tg-yw41">False</td>\n"""
                    else:
                        suiteTable += """<td class="tg-lqy6">{}</td>\n""".format("N/A")
                        suiteTable += """<td class="tg-yw4l">{}</td>\n""".format(median)
                        suiteTable += """<td class="tg-yw4l">N/A</td>\n"""
                    suiteTable += "</tr>\n"
                    j += 1
            suiteTable += "</table>\n"
            htmlTables += "\n{}\n".format(suiteTable)
    cmpFile=open(args[3], mode='w')
    cmpFile.write(htmlContent + htmlTables + "</body>\n</html>")
    cmpFile.close()

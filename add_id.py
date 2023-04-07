inFile = open(r'[INFILE LOCATION]\XBTUSD.csv', 'r')
outFilePath = r'[OUTFILE LOCATION]\xbtusd_id.csv'
clearFileWriter = open(outFilePath, 'w',)
clearFileWriter.write('')
clearFileWriter.close()
outFile = open(outFilePath, 'a',)
count = 1
while True:
    line = inFile.readline()
    if not line:
        break
    outFile.write(str(count) + ',' + line)
    count += 1

inFile.close()
outFile.close()
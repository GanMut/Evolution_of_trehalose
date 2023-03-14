#input and output arguments in python
import getopt, sys

#function definition for input and output files
def main(argv):
   inputfile = ""
   outputfile = ""
   opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   for opt, arg in opts:
      if opt == '-h':
         print ('test.py -i <inputfile> -o <outputfile>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-o", "--ofile"):
         outputfile = arg
   print ('Input file is ', inputfile)
   print ('Output file is ', outputfile)
   return inputfile,outputfile

#call function and catch the return variables
if __name__ == "__main__":
   inputFile,outputFile=main(sys.argv[1:])

#Do whatever you want
for i in open(inputFile):
        if i.startswith("("):
                j=i.strip()
out=open(outputFile,'w')
out.write(j)
out.close()

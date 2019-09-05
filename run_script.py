import os
cmd = "spark-submit jobs/puzzle.py"

returned_value = os.system(cmd)  # returns the exit code in unix
print("----------",returned_value,"------------------")
os.system("chmod 777 -R output")
if returned_value == 0:
    cmd = "cat output/temp/part-0000? >>output/final_output.txt"
    os.system(cmd)
    os.system("rm -r output/temp")


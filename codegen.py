from yaml import emit


def emit_header(outfile):
    outfile.write("import csv")
    outfile.write("")
    outfile.write("def get_value(v):")
    outfile.write("\tif v.isdigit():")
    outfile.write("\t\treturn int(v)")
    outfile.write("\telif v.isfloat():")
    outfile.write("\t\treturn float(v)")
    outfile.write("\t\telse:")
    outfile.write("\t\treturn str(v)")

def emit_footer(outfile,r):
    outfile.write("if __name__ == \"__main__\":")
    func="operator_"+str(r)
    outfile.write("for r in "+func+"():")
    outfile.write("\tprint (r)")
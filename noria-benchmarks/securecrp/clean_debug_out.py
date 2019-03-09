
def clean_file(fname, month):
    lines = []
    with open(fname, 'r') as f:
        for line in f:
            if line.startswith(month):
                words = line.split(" ")
                line = " ".join(words[4:])
            #    print line
            lines.append(line)
    with open(fname[:len(fname)-4]+"_cleaned.txt", 'w') as f:
        f.writelines(["%s" % line for line in lines])

clean_file("chair_out.txt", "Mar")
clean_file("non_chair_out.txt", "Mar")

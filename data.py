

metadata_path="meta_Books.jsonl/meta_Books.jsonl"
review_path="Books.jsonl/Books.jsonl"
def make_asin_dict(output_folder,input_file,log_ids=False,id_file="ids.csv",num_items=1000000000):
    '''
        takes an ::input_file:: .jsonl file from amazon review data and writes a file with the data in corresponding 
        files per article id(parent_asin) in ::output folder:: folder.json
        also writes all the ids to the ::id_file:: if ::log_ids:: flag is true
        uses ::num_items:: entries of the input file
    '''
    ids=set()
    with open(input_file,"r")as data:
        i=0
        for line in data:
            if (i == num_items):
                break
            if(i%1000==0):
                print(i)
            line_data= json.loads(line)
            asin = line_data["parent_asin"]
            if log_ids:
                ids.add(asin)
            with open(f"{output_folder}/{asin}.jsonl","a+") as output:
                output.write(line+"\n")
            i+=1
    if log_ids:
        with open(id_file,"w") as output:
            for asin in ids:
                output.write(asin+"\n")
        
        
def main():
    make_asin_dict("asins",metadata_path,True)
    print("now reviews")
    make_asin_dict("asins",review_path)
    #print(asin_dict).keys()
if __name__=="__main__":
    main()

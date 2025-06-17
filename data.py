import json

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

def merge_and_clean_books(book_ids,output_file,asin_folder,meta_content,review_content):
    '''
    puts all the asin files from the ::asin_folder:: together to one jsonl file ::output_file:: using all the ids 
    specified in the ::book_ids:: file
    only the keys specified in ::meta_content:: and ::review_content:: will be kept
    '''
    with open(book_ids,"r") as asins:
        for asin in asins:
            if(len(asin)>=1):
                with open(f"{asin_folder}/{asin}.jsonl","r") as data:
                    metadata = json.loads(data.readline())
                    cleaned_data={}
                    reviews = []
                    for line in data:
                        review = json.loads(line)
                        cleaned_review ={}
                        for key in review_content:
                            cleaned_review[key]=review[key]
                        reviews.append(cleaned_review)
                    cleaned_data["reviews"]=reviews
                    for key in meta_content:
                        cleaned_data[key]=metadata[key]
                with open(output_file,"a+") as output:
                    output_data={asin:cleaned_data}
                    output.write(json.dumps(output_data))                    

        
def main():
    print("logging metadata")
    #make_asin_dict("asins",metadata_path,True)
    print("logging reviews")
    make_asin_dict("asins",review_path)
    merge_and_clean_books("ids.csv","book_data.jsonl","asins",["title","average_rating","rating_number","features","description","price","categories","details"],["rating","title","text","helpful_vote","verified_purchase"])
if __name__=="__main__":
    main()

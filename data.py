import json
import pandas as pd
import os
from collections import defaultdict
import orjson

metadata_path="meta_Books.jsonl/meta_Books.jsonl"
review_path="Books.jsonl/Books.jsonl"
def make_asin_dict(output_folder,input_file,start_at=0,log_ids=False,id_file="ids.csv",num_items=1000000000):
    '''
        takes an ::input_file:: .jsonl file from amazon review data and writes a file with the data in corresponding 
        files per article id(parent_asin) in ::output folder:: folder.json
        also writes all the ids to the ::id_file:: if ::log_ids:: flag is true
        uses ::num_items:: entries of the input file
    '''
    with open(input_file,"r")as data:        
        asins=set()
        i=0
        for line in data:
            if (i == num_items):
                break
            if (i<start_at):
                i+=1
                continue
            if(i%1000==0):
                print(i)
            #load datapoint and write to corresponding file
            line_data= orjson.loads(line)
            asin = line_data["parent_asin"]
            if log_ids: 
                asins.add(asins)
            with open(f'{output_folder}/{asin}.jsonl',"a") as output:
                output.write(line+"\n")
            i+=1
    if log_ids:
        with open(id_file,"w") as output:
            for asin in asins:
                output.write(asin+"\n")

def merge_and_clean_books(book_ids,output_file,asin_folder,meta_content,review_content,start_at=0):
    '''
    puts all the asin files from the ::asin_folder:: together to one jsonl file ::output_file:: using all the ids 
    specified in the ::book_ids:: file
    only the keys specified in ::meta_content:: and ::review_content:: will be kept
    '''
    with open(book_ids,"r") as asins:
        with open(output_file,"a") as output:
            i=0
            for asin in asins:
                asin=asin.strip()
                if (i<start_at):
                    i+=1
                    continue
                if (i%1000==0):
                    print(i)
                i+=1
                if (len(asin)<=1):
                    continue #filter out empty line
                with open(f"{asin_folder}/{asin}.jsonl","r",encoding="utf-8") as data:
                    metadata = orjson.loads(data.readline().strip())
                    cleaned_data={}
                    reviews = []
                    current_timestamp=0
                    for line in data:
                        if(len(line)<=1):
                            continue #filter out emtpy lines
                        review = orjson.loads(line.strip())
                        try:
                            next_timestamp=int(review["timestamp"])
                        except KeyError:
                            next_timestamp = 0
                            print("metadata duplicate skipped")
                        #check for review duplicates
                        if next_timestamp>current_timestamp:
                            cleaned_review ={}
                            #only apply chosen keys for review objects
                            for key in review_content:
                                cleaned_review[key]=review[key]
                            reviews.append(cleaned_review)
                            current_timestamp=next_timestamp
                    cleaned_data["reviews"]=reviews
                    #only apply chosen keys for asin object
                    for key in meta_content:
                        cleaned_data[key]=metadata[key]
                    output_data={asin:cleaned_data}
                    output.write(json.dumps(output_data)+"\n")

        
def main():
    print("logging metadata")
    make_asin_dict("asins",metadata_path,log_ids=True)
    print("logging reviews")
    make_asin_dict("asins",review_path,start_at=20010000)
    print("merging files")
    merge_and_clean_books("ids.csv","book_data.jsonl","asins",["title","average_rating","rating_number","features","description","price","categories","details"],["rating","title","text","helpful_vote","verified_purchase"])
    
        
if __name__=="__main__":
    main()

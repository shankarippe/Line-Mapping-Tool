import pandas as pd,json
import re

df=pd.read_excel("RRD015 - Public Sector Deposits (2).xlsx", sheet_name='Row and Columnar Specification', skiprows=22, header=0)

df['Parent Sno'] = df['Parent Sno'].astype('Int64')    
df['SNO'] = df['SNO'].astype("Int64")

second_dataframe=pd.DataFrame({
    "SNO":df["SNO"].ffill(),
    "Line Desc":df["Line Desc"].ffill(),
    "Indicator":df["Indicator"],
    "Flag":df["Flag"],
    "Line Range Start":df["Line Range Start"],
    "Line Range End":df["Line Range End"],
})

filter_parentsno=df["Parent Sno"].isna()
filtered_df=df[filter_parentsno]

l=filtered_df["SNO"].dropna().tolist()
all_sno=df['SNO'].dropna().tolist()
#print(all_sno)
#p=df[df['SNO']==8]["Parent Sno"].tolist()
#print(p)
dict_for_heirarchy={}
track_levels_for_all_sno={}

def heirarchy_find(Serial,recurrsive_dict):
    child_sno_list=df[df['Parent Sno']==Serial]["SNO"].dropna().tolist()
    for child_ in child_sno_list:
        if df[df['Parent Sno']==child_]["SNO"].dropna().tolist():
            heirarchy_find(child_,recurrsive_dict.setdefault(child_,{}))
        else:
            recurrsive_dict[child_]=[]
    return recurrsive_dict

for parent_sno in l:
    if df[df['Parent Sno']==parent_sno]["SNO"].dropna().tolist():
        empty_dict={}
        res=heirarchy_find(parent_sno,empty_dict)
        dict_for_heirarchy.setdefault(parent_sno,res)
    else:
        dict_for_heirarchy.setdefault(parent_sno,[])


#for x,y in dict_for_heirarchy.items():
    #print(f"{x}:{y}")


level_tracker={}
max_level_count=1#for parent
max_level=1#for indicator

def settingup_levels(dicti,parent):
    global max_level_count
    for element_in_dict in dicti:
        level_count=1
        if isinstance(dicti[element_in_dict],dict):
            settingup_levels(dicti[element_in_dict],element_in_dict)
            level_count+=1
            max_level_count=max(max_level_count,level_count)
            Line_desc=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Line Desc"].iloc[0]
            line_start_ranges=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Line Range Start"].tolist()
            line_end_ranges=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Line Range End"].tolist()
            indicator=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Indicator"].iloc[0]
            flag=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Flag"].iloc[0]
            if pd.isna(indicator):
                indicator=""
            all_range_pairs=[]
            if pd.isna(line_start_ranges[0]) or pd.isna(line_end_ranges[0]):
                pass
            else:
                line_start_ranges=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Line Range Start"].dropna().tolist()
                line_end_ranges=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Line Range End"].dropna().tolist()
                for individual_ranges in range(len(line_end_ranges)):
                    store_pairs=[]
                    store_pairs.append(line_start_ranges[individual_ranges])
                    store_pairs.append(line_end_ranges[individual_ranges])
                    all_range_pairs.append(store_pairs)
            level_tracker.setdefault(element_in_dict,{"ID":element_in_dict,"levels":level_count,"parent":parent,"Line Desc":Line_desc,"ranges":all_range_pairs,"Indicator":indicator,"Flag":flag,"child":dicti[element_in_dict]})
        elif not dicti[element_in_dict]:
            Line_desc=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Line Desc"].iloc[0]
            line_start_ranges=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Line Range Start"].tolist()
            line_end_ranges=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Line Range End"].tolist()
            indicator=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Indicator"].iloc[0]
            flag=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Flag"].iloc[0]
            if pd.isna(indicator):
                indicator=""
            all_range_pairs=[]
            if pd.isna(line_start_ranges[0]) or pd.isna(line_end_ranges[0]):
                pass
            else:
                line_start_ranges=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Line Range Start"].dropna().tolist()
                line_end_ranges=second_dataframe[second_dataframe["SNO"]==element_in_dict]["Line Range End"].dropna().tolist()
                for individual_ranges in range(len(line_end_ranges)):
                    store_pairs=[]
                    store_pairs.append(line_start_ranges[individual_ranges])
                    store_pairs.append(line_end_ranges[individual_ranges])
                    all_range_pairs.append(store_pairs)
            level_tracker.setdefault(element_in_dict,{"ID":element_in_dict,"levels":1,"parent":parent,"Line Desc":Line_desc,"ranges":all_range_pairs,"Indicator":indicator,"Flag":flag,"child":[]})
        

for each_parent in dict_for_heirarchy:
    max_level_count=1
    if isinstance(dict_for_heirarchy[each_parent],dict):
        settingup_levels(dict_for_heirarchy[each_parent],each_parent)
        level_count_of_parent=max_level_count+1
        max_level=max(max_level,level_count_of_parent)
        parent_line_desc=second_dataframe[second_dataframe["SNO"]==each_parent]["Line Desc"].iloc[0]
        parent_line_start_ranges=second_dataframe[second_dataframe["SNO"]==each_parent]["Line Range Start"].tolist()
        parent_line_end_ranges=second_dataframe[second_dataframe["SNO"]==each_parent]["Line Range End"].tolist()
        parent_indicator=second_dataframe[second_dataframe["SNO"]==each_parent]["Indicator"].iloc[0]
        parent_flag=second_dataframe[second_dataframe["SNO"]==each_parent]["Flag"].iloc[0]
        if pd.isna(parent_indicator):
            parent_indicator=""
        parent_all_range_pairs=[]
        if pd.isna(parent_line_end_ranges[0]) or pd.isna(parent_line_start_ranges[0]):
            pass
        else:
            parent_line_start_ranges=second_dataframe[second_dataframe["SNO"]==each_parent]["Line Range Start"].dropna().tolist()
            parent_line_end_ranges=second_dataframe[second_dataframe["SNO"]==each_parent]["Line Range End"].dropna().tolist()
            for individual_range in range(len(parent_line_end_ranges)):
                store_pairs=[]
                store_pairs.append(parent_line_start_ranges[individual_range])
                store_pairs.append(parent_line_end_ranges[individual_range])
                parent_all_range_pairs.append(store_pairs)
        level_tracker.setdefault(each_parent,{"ID":each_parent,"levels":level_count_of_parent,"parent":0,"Line Desc":parent_line_desc,"ranges":parent_all_range_pairs,"Indicator":parent_indicator,"Flag":parent_flag,"child":dict_for_heirarchy[each_parent]})
    else:
        parent_line_desc=second_dataframe[second_dataframe["SNO"]==each_parent]["Line Desc"].iloc[0]
        parent_line_start_ranges=second_dataframe[second_dataframe["SNO"]==each_parent]["Line Range Start"].tolist()
        parent_line_end_ranges=second_dataframe[second_dataframe["SNO"]==each_parent]["Line Range End"].tolist()
        parent_indicator=second_dataframe[second_dataframe["SNO"]==each_parent]["Indicator"].iloc[0]
        parent_flag=second_dataframe[second_dataframe["SNO"]==each_parent]["Flag"].iloc[0]
        parent_all_range_pairs=[]
        if pd.isna(parent_indicator):
            parent_indicator=""
        if pd.isna(parent_line_end_ranges[0]) or pd.isna(parent_line_start_ranges[0]):
            pass
        else:
            parent_line_start_ranges=second_dataframe[second_dataframe["SNO"]==each_parent]["Line Range Start"].dropna().tolist()
            parent_line_end_ranges=second_dataframe[second_dataframe["SNO"]==each_parent]["Line Range End"].dropna().tolist()
            for individual_range in range(len(parent_line_end_ranges)):
                store_pairs=[]
                store_pairs.append(parent_line_start_ranges[individual_range])
                store_pairs.append(parent_line_end_ranges[individual_range])
                parent_all_range_pairs.append(store_pairs)
        level_tracker.setdefault(each_parent,{"ID":each_parent,"levels":1,"parent":0,"Line Desc":parent_line_desc,"ranges":parent_all_range_pairs,"Indicator":parent_indicator,"Flag":parent_flag,"child":[]})

def recursive_parent_line_desc_fetch(serial_no,fetched):
    if level_tracker[serial_no]["parent"]==0:
        line_d=level_tracker[serial_no]["Line Desc"]
        if '.' in line_d:
            final_line_d=line_d.split('.')[1]
        else:
            final_line_d=line_d
        pattern=r":|\(sum.*\)$"
        search=re.search(pattern,final_line_d)
        if search:
            delimiter_found = search.group(0)
            final_line_=final_line_d.split(delimiter_found)[0]
        else:
            final_line_=final_line_d
        final_line_=final_line_.strip()
        final_line_=final_line_.title()
        level_no=level_tracker[serial_no]["levels"]
        fetched.setdefault(f"level_{level_no}",final_line_)
        return fetched
    else:
        recursive_parent_line_desc_fetch(level_tracker[serial_no]["parent"],fetched)
        
        line_d=level_tracker[serial_no]["Line Desc"].strip()
        if '.' in line_d:
            final_line_d=line_d.split('.')[1]
        else:
            final_line_d=line_d
        pattern=r":|\(sum.*\)$"
        search=re.search(pattern,final_line_d)
        if search:
            delimiter_found = search.group(0)
            final_line_=final_line_d.split(delimiter_found)[0]
        else:
            final_line_=final_line_d
        final_line_=final_line_.strip()
        final_line_=final_line_.title()
        level_no=level_tracker[serial_no]["levels"]
        fetched.setdefault(f"level_{level_no}",final_line_)
    return fetched
ref=dict(sorted(level_tracker.items()))
#output_file_name="Line_mapping_output_008_2.json"
#with open(output_file_name,'w') as json_file:
#    json.dump(ref,json_file,indent=4)
#    print("done writnig")



entire_data_to_print=[]
for each_topic in ref:
    each_topic_data={}
    each_topic_dictionary=ref[each_topic]#getting dict for the specific topic
    sorted_level_wise_line_desc={"level_1":f"{each_topic_dictionary["Line Desc"]}"}
    if each_topic_dictionary["parent"]!=0:
        unsorted_level_wise_line_desc=recursive_parent_line_desc_fetch(each_topic_dictionary["parent"],{})
        temp=dict(sorted(unsorted_level_wise_line_desc.items(),key=lambda x:int(x[0].split('_')[1])))
        sorted_level_wise_line_desc.update(temp)
    max_level_indicator=max_level+1
    value_for_max_level=each_topic_dictionary["Indicator"]
    indicator_dict={f"level_{max_level_indicator}":value_for_max_level}
    get_flag=each_topic_dictionary["Flag"]
    flag_dict={"Flag":get_flag}
    sorted_level_wise_line_desc.update(indicator_dict)
    sorted_level_wise_line_desc.update(flag_dict)
    if not each_topic_dictionary["ranges"]:
        each_topic_data={"ID":each_topic_dictionary["ID"],"Report_Id":"CRDB_BI_015","Line_no":""}
        each_topic_data.update(sorted_level_wise_line_desc)
        
        entire_data_to_print.append(each_topic_data)
    else:
        for each_range in each_topic_dictionary["ranges"]:
            prefix,suffix=each_range[0].split('.')
            length_of_number=len(suffix)
            start=int(each_range[0].split('.')[1])
            end=int(each_range[1].split('.')[1])
            for each_number in range(start,end+1):
                full_number=f"{str(each_number).zfill(length_of_number)}"
                each_data={"ID":each_topic_dictionary["ID"],"Report_Id":"CRDB_BI_015","Line_no":f"{prefix}.{full_number}"}
                each_data.update(sorted_level_wise_line_desc)
                entire_data_to_print.append(each_data)


result_dataframe=pd.DataFrame(entire_data_to_print)
with pd.ExcelWriter("Publicsectorskeleton.xlsx") as files:
    result_dataframe.to_excel(files,sheet_name="Sheet_1",index=False)


            
            
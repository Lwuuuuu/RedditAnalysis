#Format the datapoints into a table 7x12 (7 rows for each day and 12 columns for each bihour)
#Take average across all day/bihours for each subreddit and see % better/worse alongside actual number
from plot_data import parse_rates, parse_bihour 
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import sys
Times = np.array([" ", "2:00", "4:00", "6:00", "8:00", "10:00", "12:00", "14:00", "16:00", "18:00", "20:00", "22:00", "24:00"], dtype=np.object)
Days = np.array(["SUN", "MON", "TUES", "WEDS", "THRUS", "FRI", "SAT"], dtype = str).reshape((7,1))

def cell_colors(data):
    int_array = data.astype(int)
    average = np.average(int_array)
    colors = [[] for i in range(7)]
    percentages = [[] for i in range(7)]
    rows, cols = int_array.shape
    for i in range(rows):
        for j in range(cols):
            cell = int_array[i][j]                
            dif = (cell - average) / average * 100 
            percentages[i].append(str(dif))
            if dif < 0:
                colors[i].append('red')
            else:
                colors[i].append('green') 
    for k in range(len(colors)):
        percentages[k].insert(0, " ")
        colors[k].insert(0, 'white')
    return percentages, colors, average        

def table(data, key, table_name, axes = None, index = None):
    data = np.array(data, dtype=str).reshape((7,12))
    if axes is None:
        fig, ax = plt.subplots()
        fig.patch.set_visible(False)
        ax.axis('off')
    concat = np.concatenate((Days, data), axis = 1)
    percentages, colors, average = cell_colors(data)    
    rows, cols = len(percentages), len(percentages[0])
    final_vals = [[] for i in range(rows)]
    for i in range(rows):
        for j in range(cols):
            if j != 0:
                final_vals[i].append(str(concat[i][j]) + "\n" + str(percentages[i][j][:5]) + "%")              
            else:
                final_vals[i].append(str(concat[i][j]))
    #Add to axes subplots                 
    if axes is not None:
        width = [1.2/cols for i in range(cols)]
        if index == 0:
            the_table = axes[0][0].table(cellText=final_vals, colLabels=Times, loc='center', cellColours = colors, cellLoc = 'left', colWidths = width)
            axes[0][0].axis("off")
        if index == 1:
            the_table = axes[0][1].table(cellText=final_vals, colLabels=Times, loc='center', cellColours = colors, cellLoc = 'left', colWidths = width)
            axes[0][1].axis("off")
        if index == 2:
            the_table = axes[1][0].table(cellText=final_vals, colLabels=Times, loc='center', cellColours = colors, cellLoc = 'left', colWidths = width)
            axes[1][0].axis("off")
        if index == 3: 
            the_table = axes[1][1].table(cellText=final_vals, colLabels=Times, loc='center', cellColours = colors, cellLoc = 'left', colWidths = width)
            axes[1][1].axis("off")
        the_table.auto_set_font_size(False)
        the_table.set_fontsize(6)
        #plt.tight_layout()
        return average 
    else:
        the_table = plt.table(cellText=final_vals, colLabels=Times, loc='center', cellColours = colors, cellLoc = 'left')
        the_table.auto_set_font_size(False)
        the_table.set_fontsize(6)
        plt.tight_layout()
        plt.title(table_name)
        plt.text(0, 0, "Average for " + key + " Subreddit is " + str(average)[:5])
        plt.show()
if __name__ == "__main__":
    if sys.argv[1] == "submissions_rate.txt":
        title = "Submission Rates"
        sub_list = parse_rates(sys.argv[1])
    elif sys.argv[1] == "comments_rate.txt":
        title = "Comment Rates"
        sub_list = parse_rates(sys.argv[1])
    elif sys.argv[1] == "comment_bihour.txt":
        title = "Comment Upvotes"
        sub_list = parse_bihour(sys.argv[1])
    elif sys.argv[1] == "submission_bihour.txt":
        title = "Submission Votes"
        sub_list = parse_bihour(sys.argv[1])
    else:   
        print("Error, wrong text file inputted")
        sys.exit(0)      
    for key in sub_list:
        transformed = np.array(sub_list[key], dtype=str).reshape((7,12))
        table(transformed, key, title)

    
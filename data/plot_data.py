import sys
import matplotlib.pyplot as plt 
from collections import defaultdict
time_list = [i * 2 for i in range(1,85)]
def plot_data(data, title):
    plots = []
    for i in range(1, 13):
        plots.append((4,3,i))
    plt.figure(num = None, figsize=(16,6), dpi=80, facecolor = 'w', edgecolor = 'k')
    for key,plot  in zip(data, plots):
        plt.subplot(plot[0], plot[1], plot[2])
        plt.title(key)
        plt.plot(time_list, data[key], label = key)
        plt.xlabel("Hours")
        plt.ylabel("Bi-Hourly Rate")
        plt.tight_layout()
    plt.suptitle(title)
    plt.show()
#Usage for submissions_rate.txt and comments_rate.txt
def parse_rates(file_name):
    with open(file_name) as f:
        lines = f.readlines()
        subreddit_list = defaultdict(list)
        for line in lines: 
            line = line.strip("\n").split(" ")
            subreddit_list[line[1]].append(int(line[5]))
    return subreddit_list           
def parse_bihour(file_name):
    with open(file_name) as f:
        lines = f.readlines()
        subreddit_list = defaultdict(list)
        for line in lines:
            line = line.strip("\n").split(" ")
            for string in line[1:]:
                string = string.replace("[", " ").replace("]", " ").strip(" ").strip(",")
                subreddit_list[line[0]].append(int(string))
        print(subreddit_list)            

    return subreddit_list            
if __name__ == "__main__":
    if sys.argv[1] == 'submissions_rate':
        sub_list = parse_rates('submissions_rate.txt')
        plot_data(sub_list, "Reddit Analysis on Submission Rates")
    elif sys.argv[1] == 'comments_rate':
        sub_list = parse_rates('comments_rate.txt')
        plot_data(sub_list, "Reddit Analysis on Comment Rates")
    elif sys.argv[1] == 'comment_bihour':
        sub_list = parse_bihour('comment_bihour.txt')
        plot_data(sub_list, "Reddit Analysis on Comment Upvotes")
    elif sys.argv[1] == 'submission_bihour':
        sub_list = parse_bihour('submission_bihour.txt')
        plot_data(sub_list, "Reddit Analysis on Submission Upvotes")

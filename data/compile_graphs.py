#Put all tables and graphs on to the same window 
from tables import table 
from plot_data import parse_bihour, parse_rates, plot_data
import matplotlib.pyplot as plt 
graph_type = ["Submissions", "Comments", "Submssion Upvotes", "Comment Upvotes"]
def subreddit_graphs(data, subreddit_name, which_graph):
    plots = []
    for i in range(1, 5):
        plots.append((2,2,i))
    if which_graph == 'line graph':
        plt.figure(num = None, figsize=(16,6), dpi=80, facecolor = 'w', edgecolor = 'k')
        for i in range(len(data)):
            plt.subplot(plots[i][0], plots[i][1], plots[i][2])
            plot_data(data[i][subreddit_name], subreddit_name, " ", compiling = True)
            plt.title(graph_type[i])  
        plt.suptitle(subreddit_name, y = 1)              
    elif which_graph == 'table':   
        _, axes = plt.subplots(2, 2)     
        #Sub_Rates, Comement_Rates, Sub_Bi, Comment_Bi
        pos = ((-1.2, 1.2), (0, 1.2), (-1.2, 0), (0, 0))
        for i in range(len(data)):
            average = table(data[i][subreddit_name], subreddit_name, graph_type[i], axes, i)
            plt.text(pos[i][0], pos[i][1], "Subreddit " + subreddit_name + " has an average rate of " 
            + str(average)[:6] + " " +  graph_type[i] + " per 2 hours.")
    plt.show()    
if __name__ == "__main__":
    submission_rates = parse_rates('submissions_rate.txt')
    comment_rates = parse_rates('comments_rate.txt')
    submission_bihour = parse_bihour('submission_bihour.txt')
    comment_bihour = parse_bihour('comment_bihour.txt')
    data = [submission_rates, comment_rates, submission_bihour, comment_bihour]
    for key in submission_rates:
        #subreddit_graphs(data, key, 'line graph')
        subreddit_graphs(data, key, 'table')
     

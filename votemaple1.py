import random

def maple(key, value):
    return_list = []
    for vote_list in value:
        # Take each string and interpet it as a vote
        votes = vote_list.split(',')
        for i in range(0,len(votes)-1):
            for j in range(i+1, len(votes)):
                if votes[i] < votes[j]:
                    return_list.append([votes[i]+'-'+votes[j], [1]])
                else:
                    return_list.append([votes[j]+'-'+votes[i], [0]])
    return return_list

def generate_testfiles(files):
    candidates = ['c1', 'c2', 'c3']

    for i in range(0, files):
        f = open('votesample' + str(i), 'w')
        for line in range(0, 25)
            random_ranking = random.sample(candidates, 3)
            for cand in random_ranking:
                f.write(cand + ',')
            f.write('\n')
        f.close()
from pyspark import SparkContext

def text_mapper(filePath_scrambleList):
## This function is responsible for transforming the rdd obtained from reading whole textfile
## [(('puzzle2', ('bnedl', '0,4'), '3,4,3'), =====>(filename, (srambledword, 'indices_as_comma_separated_numbers_for_final_puzzle',word_sizes_as_comma_separated_numbers_for_final_puzzle)
## ('puzzle2', ('idova', '0,3,4'), '3,4,3')]
    filePath,scrambleList = filePath_scrambleList
    wordWithIndex = ()
    filename = filePath.split("/")[-1].split(".")[0]
    scrambleListFillup = scrambleList.split("\n")
    scrambleList,fillup = scrambleListFillup[:-2],scrambleListFillup[-2]
    for item in scrambleList:
        word,index = item.split(" ")[0], item.split(" ")[1]
        wordWithIndex += ((filename,(word,index),fillup),)
    return wordWithIndex

def solvePuzzle(file_scrambledWordindex):
    ## This is a (narraw Transformation, one to many transformation) which returns generator 
    ## generates all posiible valid unscrambled word from each scrambled word 
    ##[(0, 'puzzle2', ('blend', 6253), '0,4'),
    ##(1, 'puzzle2', ('avoid', 2195), '0,3,4')]
    file,scrambledWordindex,puzzleId = file_scrambledWordindex
    scrambledWord,index = scrambledWordindex 
    word_rank_dict = word_rank.value
    from itertools import permutations 
    permList = permutations(scrambledWord) 
    tempWordSet =set()
    for word in permList:
        tempWord = "".join(word) 
        if tempWord in word_rank_dict:
            tempWordSet.add((tempWord,word_rank_dict[tempWord]))
    setSize = len(tempWordSet)
    for word in tempWordSet:
        yield (puzzleId,file, word,index)
#         
def possibleLetterCombination(lettersList):
    ## After unscrabled word/ words are found, each words mentioned letters are picked 
    ##and all possible letters list is made 
    ## ('puzzle2', ['b', 'd', 'a', 'i', 'd', 'h', 'y', 'a', 'r', 'a']),
    ## ('puzzle2', ['b', 'd', 'a', 'i', 'd', 'h', 'y', 'c', 'a', 'm']),
    ## Two puzzle2 is because while unscrambling the individual puzzle mutiple possible words were possible
    puzzle,letters = lettersList
    import itertools 
    letterCombinations = itertools.product(*letters)
    for lst in letterCombinations:
        flatList = [item for sublist in lst for item in sublist]
        yield(puzzle,flatList)

def possibleWordsFinalPuzzle(puzzle_letters):
    ## All possible final wordlist is prepared 
    ## [
    ##  (puzzle2,[[word list for first fill up],[word list for first fill up],[word list for first fill up],.....,[last fill up]]),
    ##  (puzzle2,[[word list for first fill up],[word list for first fill up],[word list for first fill up],.....,[last fill up]]),
    ##   multiple tuples for puzzle is possible becasue of more than one solution as we had more than one solution for individual unscrambling problem
    ##]
    word_rank_dict = word_rank.value
    from itertools import permutations
    puzzle_word_size_dict = broadcast_puzzle_word_size.value
    puzzleId,letters = puzzle_letters
    visited_size_set = set()
    wordsList = []
    sizes = puzzle_word_size_dict[puzzleId]
    count = 0
    for size in sizes:
        count += 1
        tempSet=set()
        words = permutations(letters,size)
        if not size in visited_size_set:
            visited_size_set.add(size)
            for letters_array in words:
                word = "".join(letters_array)
                if word in word_rank_dict and word_rank_dict[word]:
                    tempSet.add("".join(word))
            wordsList.append(list(tempSet))
        else:
            firstIndexOfSize = sizes.index(size)
            wordsList.append(wordsList[firstIndexOfSize])
    return (puzzleId,wordsList)
            

def assignWeight(puzzleIdWordLists):
    ## Since there is more than one possible solution for final fillup problem we decide using the weights of words 
    ## For now I have used more popular words for every tuple inside the lists andd added the raning number to get the weight of the fillups
    import math
    puzzleId,wordLists = puzzleIdWordLists
    word_rank_dict = word_rank.value
    finalWordCombo=[]
    for wordList in wordLists:
        tempWord=""
        wordRank=math.inf
        for word in wordList:
            if (not word in finalWordCombo) and word_rank_dict[word]< wordRank:
                tempWord = word
                wordRank = word_rank_dict[word]
        finalWordCombo.append((tempWord))
    totalWeight = sum(word_rank_dict[i] for i in finalWordCombo)
    return(puzzleId,[[totalWeight,finalWordCombo]])
            
    
    

def selectMaxWeightedCombo(puzzleIdWeightAndWords):
    ## After gaining the weight of possible combination of words
    ## experimentally I found taking highest weighted sum gives more close to results thereby avoiding all words being stop words 
    puzzleId,weightAndWordsLists= puzzleIdWeightAndWords
    maxWeight = -1
    maxWeightedList = []
    for weightAndWordsList in weightAndWordsLists:
        weight,wordsList = weightAndWordsList
        if maxWeight < weight:
            maxWeight = weight
            maxWeightedList = wordsList
    return(puzzleId,maxWeightedList)



if __name__=="__main__":
    sc = SparkContext()
    sourcePath = "../data/*"
    wordDictPath = "../resources/freq_dict.json"
    scrambledRdd = sc.wholeTextFiles(sourcePath)
    import json
    word_rank_dict = {}
    ##Load json file as dictionary
    with open(wordDictPath, 'r') as f:
            word_rank_dict = json.load(f)
    ## end loading json file as dictionary        
    word_rank = sc.broadcast(word_rank_dict) #### broadcast the dictionary to all the executor nodes
    puzzleWithIdWithFillupRdd = scrambledRdd.map(text_mapper).flatMap(lambda x:x).zipWithIndex()
    idWithFillupRdd = puzzleWithIdWithFillupRdd.map(lambda x: (x[0][0],x[0][2]))

    idWithFillup = idWithFillupRdd.distinct().collect()
    ## collect the final puzzle fillups words sizes 
    ##[('puzzle2', '3,4,3'), ('puzzle1', '3,4,4')]
    
    ##parse the above collected list to dictionary
    ## to get {'puzzle2': [3, 4, 3], 'puzzle1': [3, 4, 4]}
    puzzle_word_size={}
    for tup in idWithFillup:
        temp = [int(i) for i in tup[1].split(",")]
        puzzle_word_size[tup[0]]= temp
    broadcast_puzzle_word_size = sc.broadcast(puzzle_word_size)
    broadcast_puzzle_word_size.value
    ## broadcast the dictionary {'puzzle2': [3, 4, 3], 'puzzle1': [3, 4, 4]} across the executors 

    unscrambledRdd = puzzleWithIdWithFillupRdd.map(lambda x:(x[0][0],x[0][1],x[1])).flatMap(solvePuzzle)
    unscrambledForFinalJoinRdd= unscrambledRdd.map(lambda x:(x[1],[x[2][0]])).reduceByKey(lambda x,y:x+y)
    
    puzzledFinalLettersRdd = unscrambledRdd.map(lambda x:x[:-1]+ (tuple([ int(i) for i in x[-1].split(",")]),)).map(lambda x:((x[0],x[1]),([[x[2][0][i] for  i in x[3] ]])))                            .reduceByKey(lambda x,y:x+y)                             .map(lambda x: (x[0][1],[x[1]]))                             .reduceByKey(lambda x,y:x+y)                             .flatMap(possibleLetterCombination)                             .map(possibleWordsFinalPuzzle)                             .map(assignWeight)
    
    puzzledFinalLettersRddForJoin = puzzledFinalLettersRdd.reduceByKey(lambda x,y:x+y).map(selectMaxWeightedCombo)
    ## save as textFile in the output directory
    
    unscrambledForFinalJoinRdd.join(puzzledFinalLettersRddForJoin).saveAsTextFile("../output/abc")
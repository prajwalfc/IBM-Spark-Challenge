from pyspark import SparkContext

def text_mapper(filePath_scrambleList):
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
    puzzle,letters = lettersList
    import itertools 
    letterCombinations = itertools.product(*letters)
    for lst in letterCombinations:
        flatList = [item for sublist in lst for item in sublist]
        yield(puzzle,flatList)

def possibleWordsFinalPuzzle(puzzle_letters):
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
    with open(wordDictPath, 'r') as f:
            word_rank_dict = json.load(f)
    word_rank = sc.broadcast(word_rank_dict)
    puzzleWithIdWithFillupRdd = scrambledRdd.map(text_mapper).flatMap(lambda x:x).zipWithIndex()
    puzzleWithIdWithFillupRdd.take(10)
    idWithFillupRdd = puzzleWithIdWithFillupRdd.map(lambda x: (x[0][0],x[0][2]))

    idWithFillup = idWithFillupRdd.distinct().collect()
    idWithFillup
    puzzle_word_size={}
    for tup in idWithFillup:
        temp = [int(i) for i in tup[1].split(",")]
        puzzle_word_size[tup[0]]= temp
    broadcast_puzzle_word_size = sc.broadcast(puzzle_word_size)
    broadcast_puzzle_word_size.value
    unscrambledRdd = puzzleWithIdWithFillupRdd.map(lambda x:(x[0][0],x[0][1],x[1])).flatMap(solvePuzzle)
    unscrambledForFinalJoinRdd= unscrambledRdd.map(lambda x:(x[1],[x[2][0]])).reduceByKey(lambda x,y:x+y)
    
    puzzledFinalLettersRdd = unscrambledRdd.map(lambda x:x[:-1]+ (tuple([ int(i) for i in x[-1].split(",")]),)).map(lambda x:((x[0],x[1]),([[x[2][0][i] for  i in x[3] ]])))                            .reduceByKey(lambda x,y:x+y)                             .map(lambda x: (x[0][1],[x[1]]))                             .reduceByKey(lambda x,y:x+y)                             .flatMap(possibleLetterCombination)                             .map(possibleWordsFinalPuzzle)                             .map(assignWeight)
    puzzledFinalLettersRdd.take(40)

    puzzledFinalLettersRddForJoin = puzzledFinalLettersRdd.reduceByKey(lambda x,y:x+y).map(selectMaxWeightedCombo)
    unscrambledForFinalJoinRdd.join(puzzledFinalLettersRddForJoin).saveAsTextFile("../output/abc")
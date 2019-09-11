#!/usr/bin/env python
# coding: utf-8

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



def possibleLetterCombination(lettersList):
    puzzle,letters = lettersList
    import itertools 
    letterCombinations = itertools.product(*letters)
    for lst in letterCombinations:
        flatList = [item for sublist in lst for item in sublist]
        yield(puzzle,flatList)


def possibleWordsFinalPuzzle(puzzle_letters):
    word_rank_dict = word_rank.value    #### dictionary of words
    from itertools import permutations
    puzzle_word_size_dict = broadcast_puzzle_word_size.value     ### fillup word size
    puzzleId,letters = puzzle_letters
    puzzle = puzzleId[0]
    if len(letters)> sum(puzzle_word_size_dict[puzzle]):
        print("rejected------",puzzleId)
        return
    sizes = puzzle_word_size_dict[puzzle]
    lettersPermutation = permutations(letters)
    count=0
    for permutedLetters in lettersPermutation:
        stringWord = "".join(permutedLetters)
        prev_size = 0
        finalWords = []
        count+=1
        for size in sizes:
            word = stringWord[prev_size:prev_size+size]
            if word in word_rank_dict :
                if word_rank_dict[word]:
                    finalWords.append(word)
            else:
                break
            prev_size = prev_size+size
        if len(finalWords)==len(sizes):
                yield puzzleId,finalWords
    


def extractWordIdToSeparateList(args):
    puzzleId,letters = args
    t =list()
    s=[]
    collectedId = [i for i in letters if type(i) == int]
    collectedLetters = [i for i in letters if not type(i) is int]
    return puzzleId,collectedId,collectedLetters



#('puzzle2',([0, 1, 2, 4],['bay', 'hair', 'dad'],(2283.6666666666665, 1556.761239097234)))
def mapIndexToWord(puzzleIndexFillup):
    puzzleId,fillup = puzzleIndexFillup
    wordIndex,fillupWords,_ = fillup
    possibleUnscrambledMap = broadcastPossibleUnscrabbledMap.value
    words = [possibleUnscrambledMap[i] for i in wordIndex]
    return puzzleId,words,fillupWords
    


# In[8]:


def wordRankSimilarityMapper(puzzleIndexFillup):
    puzzleIndex,fillupList = puzzleIndexFillup
    puzzleId,*wordIndex  = puzzleIndex
    word_rank_dict = word_rank.value
    fillupWordRankList = [word_rank_dict[word] for word in fillupList]
    import numpy as np  
    mean,sigma = np.mean(fillupWordRankList),np.std(fillupWordRankList)
    return puzzleId,(wordIndex,fillupList,(mean,sigma))
    


# # Main Program



if __name__=="__main__":
    sc = SparkContext()
    sourcePath = "data/*"

    # read the input file as a whole with key as filename path and value as contents of correspong file
    scrambledRdd = sc.wholeTextFiles(sourcePath)

    # get the word rank json file as dictionary and broadcast to all the executors
    import json
    with open("resources/freq_dict.json", 'r') as f:
            word_rank = sc.broadcast(json.load(f))

    # map the file path to just name of file without extension as key and contents as value
    # flatten the contents so that each unscrambling puzzle is a record in rdd
    # index each unscrabling puzzle with unique id
    puzzleWithIdWithFillupRdd = scrambledRdd.map(text_mapper).flatMap(lambda x:x).zipWithIndex()
    # output------------->(('puzzle2', ('bnedl', '0,4'), '3,4,3'), 0),
    #                      (('puzzle2', ('idova', '0,3,4'), '3,4,3'), 1),.....)

    # for each puzzle file get the final fillup word sizes
    idWithFillupRdd = puzzleWithIdWithFillupRdd.map(lambda x: (x[0][0],x[0][2]))
    # ('puzzle2', '3,4,3'),
    #  ('puzzle2', '3,4,3'),

    # collect distinct puzzle word sizes
    idWithFillup = idWithFillupRdd.distinct().collect()
    #[('puzzle2', '3,4,3'), ('puzzle1', '3,4,4')]

    # broadcast the final fillup word size that will be need some time later
    puzzle_word_size={}
    for tup in idWithFillup:
        temp = [int(i) for i in tup[1].split(",")]
        puzzle_word_size[tup[0]]= temp
    broadcast_puzzle_word_size = sc.broadcast(puzzle_word_size)
    # broadcast_puzzle_word_size.value   {'puzzle2': [3, 4, 3], 'puzzle1': [3, 4, 4]}

    # map the incoming tuples to excluse final word size
    # solve the unscrambling puzzle and flat map in case some unscrambling puzzle has multiple solutions
    # so that each solution be trated as separate
    unscrambledRdd = puzzleWithIdWithFillupRdd.map(lambda x:(x[0][0],x[0][1],x[1])).flatMap(solvePuzzle)
    # (0, 'puzzle2', ('blend', 6253), '0,4'),
    #  (1, 'puzzle2', ('avoid', 2195), '0,3,4'),
    #  (2, 'puzzle2', ('cheesy', 0), '1,5'),

    # index each unscrambled solution to unizue value to represent solved word as wordId
    unscrambledRddWithIndex = unscrambledRdd.zipWithIndex().map(lambda x:x[0]+(x[1],))
    # (0, 'puzzle2', ('blend', 6253), '0,4', 0),
    #  (1, 'puzzle2', ('avoid', 2195), '0,3,4', 1),
    #  (2, 'puzzle2', ('cheesy', 0), '1,5', 2),

    ## Parse string index of selected letters from solved unscrambled words to tuple
    unscrambledRddWithLetterIndex = unscrambledRddWithIndex.map(lambda x:x[:-2]+ (tuple([ int(i) for i in x[-2].split(",")]),x[-1]))
    # [(0, 'puzzle2', ('blend', 6253), (0, 4), 0),
    #  (1, 'puzzle2', ('avoid', 2195), (0, 3, 4), 1),

    # select the letters from unscrambled words with indexes, drop those words then 
    # and mix the letters with wordId of unscrambled words
    # some of the unscrable puzzles has multiple answers
    puzzleSelectedLettersWordId = unscrambledRddWithLetterIndex.map(lambda x:((x[0],x[1]),([[x[2][0][i] for  i in x[3] ]+[x[-1]]])))
    # [((0, 'puzzle2'), [['b', 'd', 0]]),
    #  ((1, 'puzzle2'), [['a', 'i', 'd', 1]]),

    # Since sum of the unscrabble puzzle has multiple answers
    # combine them using reduceByKey operation
    puzzleSelectedLettersWordIdCombined = puzzleSelectedLettersWordId.reduceByKey(lambda x,y:x+y)
    # ((0, 'puzzle2'), [['b', 'd', 0]]),
    #  ((1, 'puzzle2'), [['a', 'i', 'd', 1]]),
    #  ((2, 'puzzle2'), [['h', 'y', 2], ['y', 'e', 3]]),

    # drop The puzzleId from the key we dont need any more
    finalPossiblePuzzleWithLettersAndWordId = puzzleSelectedLettersWordIdCombined.map(lambda x: (x[0][1],[[i for i in x[1]]]))
    # ('puzzle2', [[['b', 'd', 0]]]),
    #  ('puzzle2', [[['a', 'i', 'd', 1]]]),
    #  ('puzzle2', [[['h', 'y', 2], ['y', 'e', 3]]]),

    # Combine the list of letters with source words for each final puzzle
    finalPuzzleWithLettersWithSOuceWordId = finalPossiblePuzzleWithLettersAndWordId.reduceByKey(lambda x,y:x+y)
    # [('puzzle2',
    #   [[['b', 'd', 0]],
    #    [['a', 'i', 'd', 1]],
    #    [['h', 'y', 2], ['y', 'e', 3]],
    #    [['a', 'r', 'a', 4], ['c', 'a', 'm', 5], ['a', 'c', 'a', 6]]]),
    #  ('puzzle1',
    #   [[['l', 'n', 'd', 7]],
    #    [['r', 'a', 8], ['j', 'o', 9], ['r', 'm', 10]],
    #    [['b', 'e', 'a', 11]],
    #    [['w', 'r', 'l', 12], ['l', 'w', 'e', 13], ['y', 'w', 'e', 14]]])]

    # combine list of letters with souce wordId for all possible combinations for each puzzle
    possibleLetterCombinationWithSourceWordId = finalPuzzleWithLettersWithSOuceWordId.flatMap(possibleLetterCombination)
    # [('puzzle2', ['b', 'd', 0, 'a', 'i', 'd', 1, 'h', 'y', 2, 'a', 'r', 'a', 4]),
    #  ('puzzle2', ['b', 'd', 0, 'a', 'i', 'd', 1, 'h', 'y', 2, 'c', 'a', 'm', 5]),
    #  ('puzzle2', ['b', 'd', 0, 'a', 'i', 'd', 1, 'h', 'y', 2, 'a', 'c', 'a', 6]),
    #  .....
    #  ('puzzle1',
    #   ['l', 'n', 'd', 7, 'r', 'a', 8, 'b', 'e', 'a', 11, 'l', 'w', 'e', 13]),
    #  ('puzzle1',
    #   ['l', 'n', 'd', 7, 'r', 'a', 8, 'b', 'e', 'a', 11, 'y', 'w', 'e', 14]),
    #  ('puzzle1',
    #   ['l', 'n', 'd', 7, 'j', 'o', 9, 'b', 'e', 'a', 11, 'w', 'r', 'l', 12]),

    # extract source wordId from letter list to separate list 
    # to keep track of source word from which the letters derived
    puzzleSourceWordIdLetters = possibleLetterCombinationWithSourceWordId.map(extractWordIdToSeparateList)
    # ('puzzle2', [0, 1, 2, 4], ['b', 'd', 'a', 'i', 'd', 'h', 'y', 'a', 'r', 'a']),
    # ('puzzle2', [0, 1, 2, 5], ['b', 'd', 'a', 'i', 'd', 'h', 'y', 'c', 'a', 'm']),
    # ('puzzle1',[7, 8, 11, 12],['l', 'n', 'd', 'r', 'a', 'b', 'e', 'a', 'w', 'r', 'l']),
    # ('puzzle1',[7, 8, 11, 13],['l', 'n', 'd', 'r', 'a', 'b', 'e', 'a', 'l', 'w', 'e']),

    # combine puzzle with source wordId list as single key data structure
    puzzleSourceWordIdAsKeyLetters  = puzzleSourceWordIdLetters.map(lambda x: (tuple([x[0]]+x[1]),x[2]))
    #  (('puzzle2', 0, 1, 3, 5), ['b', 'd', 'a', 'i', 'd', 'y', 'e', 'c', 'a', 'm']),
    #  (('puzzle2', 0, 1, 3, 6), ['b', 'd', 'a', 'i', 'd', 'y', 'e', 'a', 'c', 'a']),
    #  (('puzzle1', 7, 8, 11, 12),['l', 'n', 'd', 'r', 'a', 'b', 'e', 'a', 'w', 'r', 'l']),
    #  (('puzzle1', 7, 8, 11, 13),['l', 'n', 'd', 'r', 'a', 'b', 'e', 'a', 'l', 'w', 'e']),

    # taking the letters of list from every puzzle with source word id as key,
    # go through all the permutaion of arangement of letters for each puzzle with source wordId
    # split the each permuted string into word size as required in final puzzle fillup
    # check if any one of the word not in dictionary, reject the permutaion
    puzzleSourceWordIdFormedWords = puzzleSourceWordIdAsKeyLetters.flatMap(possibleWordsFinalPuzzle)
    # (('puzzle2', 0, 1, 2, 4), ['bay', 'hair', 'dad']),
    #  (('puzzle2', 0, 1, 2, 4), ['bay', 'hair', 'dad']),

    # now that I have list of final words for each puzzle
    # need to make a choice 
    # not very best way available to check which words best fits in the sentence that is consistent with context
    # and english grammar
    # just for convenience I chose to choose words that lies in similar rank by taking list of words whose ranks had 
    # minimum standard deviation
    puzzleSourceWordIdFormedWordsWithSigma = puzzleSourceWordIdFormedWords.map(wordRankSimilarityMapper)


    # for each puzzle grab the word list which had minimum standard deviation
    solvedPuzzleWithSourceWordId= puzzleSourceWordIdFormedWordsWithSigma.reduceByKey(lambda x1, x2: min(x1, x2, key=lambda x: x[-1][-1]))
    # [('puzzle2',([0, 1, 3, 4],['bay', 'area', 'did'],(501.6666666666667, 368.03109771986504))),
    # ('puzzle1',([7, 8, 11, 13], ['raw', 'dean', 'bell'], (3100.0, 86.91758548571553)))]

    possibleUnscrabbledMap = unscrambledRddWithIndex.map(lambda x: (x[-1],x[2][0])).collectAsMap()
    broadcastPossibleUnscrabbledMap = sc.broadcast(possibleUnscrabbledMap)
    # broadcastPossibleUnscrabbledMap.value
    # {0: 'blend',
    #  1: 'avoid',
    #  2: 'cheesy',
    #  3: 'sychee',
    #  4: 'camera',
    #  5: 'acream',
    #  6: 'mareca',
    #  7: 'gland',
    #  8: 'joram',
    #  9: 'major',
    #  10: 'jarmo',
    #  11: 'becalm',
    #  12: 'warely',
    #  13: 'lawyer',
    #  14: 'yawler'}

    finalAnswer = solvedPuzzleWithSourceWordId.map(mapIndexToWord)
    finalAnswer.saveAsTextFile("output/temp")


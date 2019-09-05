# IBM-Spark-Challenge
# Question
## Jumble Solver:
-The jumble puzzle is a common newspaper puzzle, it contains a series of anagrams that must be solved  
(see -https://en.wikipedia.org/wiki/Jumble). To solve, one must solve each of the individual jumbles. The circled letters are  then -used to create an additional anagram to be solved. In especially difficult versions, some of the anagrams in the first  set can possess multiple solutions. To get the final answer, it is important to know all possible anagrams of a given series of  letters.

## Included
Also included:freq_dict - keys are English Dictionary words to be used in your solving of the jumbles. 
Non-zero values are the frequency rankings (1=most frequent). Zero values mean that the word is too infrequent to be ranked. 

## Environment
-Linux
## Installing Spark (unix and linux)
- Step 1 : [Install Anaconda](https://www.anaconda.com/distribution/#download-section)
- Step 2: conda install pyspark
- run pyspark command in terminal, that should load pyspark shell.

# Sample Input Picture
![alt text](images/puzzle1.jpg)

## Future Todos
- Chossing the only words that results in most appropriate words in final fillup puzzle(i.e go back and remove redundant words)   right now all possible words from the dictionary are included. 
- Tune the number to best select the word out of possible combination
- Use NLP to select best word in context of the sentence.(Right now the contextual sentence is ignored)
- Improve output format
- Work on to make production level and modular jobs

## Running the application
- git clone https://github.com/prajwalfc/IBM-Spark-Challenge.git
- python run_script.py
- remove the output file inside output folder.

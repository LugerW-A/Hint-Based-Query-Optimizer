class MCTS:
    def __init__(self):
        file = open('./leading.txt', 'r')
        self.leadings = file.readlines()
        file.close() 

    def find_leading(self, n):
        return self.leadings[n]
class set_arm:
    def __init__(self):
        self.enable_hashjoin = False
        self.enable_mergejoin = False
        self.enable_nestloop = False
        self.enable_indexscan = False
        self.enable_seqscan = False
        self.enable_indexonlyscan = False
    def set_arm_options(self, arm):
        self.enable_hashjoin = False
        self.enable_mergejoin = False
        self.enable_nestloop = False
        self.enable_indexscan = False
        self.enable_seqscan = False
        self.enable_indexonlyscan = False
        if arm == 0:
            self.enable_hashjoin = True;
            self.enable_indexscan = True;
            self.enable_mergejoin = True;
            self.enable_nestloop = True;
            self.enable_seqscan = True;
            self.enable_indexonlyscan = True;     
        elif arm == 1: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_indexscan = True; 
            self.enable_mergejoin = True; 
            self.enable_seqscan = True; 
        elif arm == 2: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_nestloop = True; 
            self.enable_seqscan = True; 
        elif arm == 3: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_seqscan = True; 
        elif arm == 4: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_indexscan = True; 
            self.enable_nestloop = True; 
            self.enable_seqscan = True; 
        elif arm == 5: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_mergejoin = True; 
            self.enable_nestloop = True; 
        elif arm == 6: 
            self.enable_hashjoin = True; 
            self.enable_indexscan = True; 
            self.enable_mergejoin = True; 
            self.enable_nestloop = True; 
        elif arm == 7: 
            self.enable_indexonlyscan = True; 
            self.enable_mergejoin = True; 
            self.enable_nestloop = True; 
        elif arm == 8: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
        elif arm == 9: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_indexscan = True; 
            self.enable_nestloop = True; 
        elif arm == 10: 
           self.enable_hashjoin = True; 
           self.enable_indexonlyscan = True; 
           self.enable_indexscan = True; 
           self.enable_seqscan = True; 
        elif arm == 11: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_mergejoin = True; 
            self.enable_nestloop = True; 
            self.enable_seqscan = True; 
        elif arm == 12:  
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_mergejoin = True; 
            self.enable_seqscan = True; 
        elif arm == 13: 
            self.enable_hashjoin = True; 
            self.enable_indexscan = True; 
            self.enable_nestloop = True; 
        elif arm == 14: 
            self.enable_indexscan = True; 
            self.enable_nestloop = True; 
        elif arm == 15: 
            self.enable_indexscan = True; 
            self.enable_mergejoin = True; 
            self.enable_nestloop = True; 
            self.enable_seqscan = True; 
        elif arm == 16: 
            self.enable_indexonlyscan = True; 
            self.enable_indexscan = True; 
            self.enable_nestloop = True; 
        elif arm == 17: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_indexscan = True; 
            self.enable_mergejoin = True; 
            self.enable_nestloop = True; 
        elif arm == 18: 
            self.enable_indexscan = True; 
            self.enable_mergejoin = True; 
            self.enable_nestloop = True; 
        elif arm == 19: 
            self.enable_indexonlyscan = True; 
            self.enable_mergejoin = True; 
            self.enable_nestloop = True; 
            self.enable_seqscan = True; 
        elif arm == 20: 
            self.enable_indexonlyscan = True; 
            self.enable_indexscan = True; 
            self.enable_nestloop = True; 
            self.enable_seqscan = True; 
        elif arm == 21: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_indexscan = True; 
            self.enable_mergejoin = True; 
        elif arm == 22: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_mergejoin = True; 
        elif arm == 23: 
            self.enable_hashjoin = True; 
            self.enable_indexscan = True; 
            self.enable_nestloop = True; 
            self.enable_seqscan = True; 
        elif arm == 24: 
            self.enable_hashjoin = True; 
            self.enable_indexscan = True; 
        elif arm == 25: 
            self.enable_hashjoin = True; 
            self.enable_indexonlyscan = True; 
            self.enable_nestloop = True; 
        else:
            print("Out of index")
            exit(0)
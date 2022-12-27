from itertools import combinations
from collections import namedtuple

class TransactionManager(object):

    def __init__(self,transactions):

        self.__num_transaction = 0
        self.__item = []
        self.__transaction_index_map = {}
        
        for transaction in transactions:
            self.add_transaction(transaction)

    def add_transaction(self, transaction):
        # transaction = ['a','b','c']
        for item in transaction:
            if item not in self.__transaction_index_map:
                self.__item.append(item)
                self.__transaction_index_map[item]= set()
            self.__transaction_index_map[item].add(self.__num_transaction)
        self.__num_transaction += 1
    
    def calc_transaction(self, items):
        
        if not items:
            return self.__num_transaction
        
        if not self.num_transaction:
            return 0
        
        sum_indexes = None
        for item in items:

            indexes = self.__transaction_index_map.get(item)

            if indexes is None:
                 return 0

            if sum_indexes is None:
                sum_indexes = indexes
            else:
                sum_indexes = sum_indexes.intersection(indexes)

        return len(sum_indexes)
    
    @property
    def items(self):
        # ex. __item = ['a','b','c']
        return sorted(self.__item)

    @property
    def num_transaction(self):
        return self.__num_transaction
    
    @staticmethod
    def create(transactions):

        if isinstance(transactions, TransactionManager):
            return transactions
        return TransactionManager(transactions)

TransactionRecord = namedtuple(
    'TransactionRecord', (
        'items','transaction_itemset', 'transaction_itemsets'))
OrderedStatistic = namedtuple(
    'OrderedStatistic', (
        'items_base', 
        'transaction_items_base', 
        'items_add', 
        'transaction_items_add',
        'confidence', 
        'lift'))
RelationRecord = namedtuple(
    'RelationRecord', TransactionRecord._fields + ('ordered_statistics',))

def create_length_is_two_candidates(prev_candidates):
    
    # The candidates of length of the candidates is 2
    tmp_length_is_two_candidates = (
        frozenset(x) for x in combinations(prev_candidates, 2))
    return tmp_length_is_two_candidates

def gen_transaction_records(transaction_manger):

    candidates = transaction_manger.items
    candidates = create_length_is_two_candidates(candidates)

    for relation_candidate in candidates:
        transaction = transaction_manger.calc_transaction(relation_candidate)
        if transaction == 0:
            continue
        candidate_set = frozenset(relation_candidate)
        yield TransactionRecord(
            candidate_set,transaction,transaction_manger.num_transaction)

def gen_ordered_statistic(transaction_manager,record):

    items = record.items
    sorted_items = sorted(items)
    for combination_set in combinations(sorted_items, 1):
        items_base = frozenset(combination_set)
        items_add = frozenset(items.difference(items_base))
        transaction_items_base = transaction_manager.calc_transaction(items_base)
        transection_items_add = transaction_manager.calc_transaction(items_add)
        confidence = (record.transaction_itemset / record.transaction_itemsets) / (
            transaction_items_base / record.transaction_itemsets)
        lift = confidence / (
            transection_items_add / record.transaction_itemsets)
        yield OrderedStatistic(
            frozenset(items_base), transaction_items_base, frozenset(items_add), transection_items_add, confidence, lift)

def apriori(transactions):
    
    transaction_manager = TransactionManager.create(transactions)

    transaction_records = gen_transaction_records(transaction_manager)

    for transaction_record in transaction_records:
        ordered_statistic = list(
            gen_ordered_statistic(transaction_manager,transaction_record))
        if not ordered_statistic:
            continue
        yield RelationRecord(
            transaction_record.items,
            transaction_record.transaction_itemset,
            transaction_record.transaction_itemsets,
            ordered_statistic
            )

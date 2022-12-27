def transform_to_transactions(payment_no,product):
    # parameter = [['1', 'a'],[']]
    itemsets = []
    products = []
    for i in range(len(product)):
        if i+1 == len(product):
            products.append(product[i])
            itemsets.append(products)
        elif payment_no[i] == payment_no[i+1]:
            products.append(product[i])
        else:
            products.append(product[i])
            itemsets.append(products)
            products = []
    return itemsets

def manage_rule(recive_rule):
    for rule in recive_rule:
        transaction_itemset = rule.transaction_itemset
        transaction_itemsets = rule.transaction_itemsets
        for partition in rule.ordered_statistics:
            items_base = list(partition.items_base)
            items_base = items_base[0]
            items_add = list(partition.items_add)
            items_add = items_add[0]
            transaction_items_base = partition.transaction_items_base
            transaction_items_add = partition.transaction_items_add
            confidence = partition.confidence
            lift = partition.lift
            yield (
                items_base,
                items_add,
                transaction_itemsets,
                transaction_itemset,
                transaction_items_base,
                transaction_items_add,
                lift,
                confidence)

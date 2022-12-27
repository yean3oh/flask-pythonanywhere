from flask_sqlalchemy import SQLAlchemy
from flask import Flask,request
from flask_restful import marshal_with,abort,fields,Api,Resource
import pandas as pd
from myTools import transform_to_transactions,manage_rule
from apriori_length_is_two import apriori
from threading import Thread
from flask_cors import CORS
db = SQLAlchemy()
app = Flask (__name__)
CORS(app)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///mydatabase.db"
db.init_app(app)
api=Api(app)

class Rule(db.Model):
    id=db.Column(db.Integer, primary_key=True)
    items_base = db.Column(db.String(100), nullable=False)
    items_add = db.Column(db.String(100), nullable=False)
    transaction_itemsets = db.Column(db.Integer, nullable=False)
    transaction_itemset = db.Column(db.Integer, nullable=False)
    transaction_items_base = db.Column(db.Integer, nullable=False)
    transaction_items_add = db.Column(db.Integer, nullable=False)
    lift = db.Column(db.Float, nullable=False)
    confidence = db.Column(db.Float, nullable=False)
with app.app_context():
    db.create_all()

resource_field = {
    "items_base":fields.String,
    "items_add":fields.String,
    "transaction_itemsets":fields.Integer,
    "transaction_itemset":fields.Integer,
    "transaction_items_base":fields.Integer,
    "transaction_items_add":fields.Integer,
    "lift":fields.Float,
    "confidence":fields.Float
}
class RecommendationSystem(Resource):
    
    @marshal_with(resource_field)
    def get(self,product_id,lift=0,confidence=0):
        if product_id == 'all':
            result = Rule.query.all()
            if not result:
                abort(404,message="ไม่พบรายการ")
            return result
        result = Rule.query.filter_by(items_base= product_id).filter((Rule.lift > lift) & (Rule.confidence > confidence) ).order_by(Rule.confidence.desc()).all()
        if not result:
            abort(404,message="ไม่พบรายการ")
        return result

status = 0

class DatabaseManager(Resource):

    def get(self,cmd=None):
        global status
        if cmd == 'reset':
            if status == "API create error DatabaseManager ERROR" or status == "API add error DatabaseManager ERROR":
                status = 0
                return {"message":'Available'},200
            else:
                abort(400,message="Can not reset while create or add data")
        if status == "API create error DatabaseManager ERROR" or status == "API add error DatabaseManager ERROR":
            abort(500,message=status)
        elif not status:
            return {"message":'Available'},200
        else:
            return{"message": status},200
        
    def post(self,cmd=None,payment_no='PAYMENT_NO',product_id='PRODUCT_ID'):
        data_json = request.get_json()
        global status
        try:
            df = pd.DataFrame(data_json)
            df = df.sort_values(by=[payment_no])
            payment_no = [x for x in df[payment_no]]
            product_id = [x for x in df[product_id]]
        except:
            abort(400,message="ข้อมูลไม่ตรงตามที่กำหนด")
        if cmd == "create":
            if not status:
                db.drop_all()
                db.create_all()
                status = "Creating Database"
                t = Thread(target=do_create_command, args=(payment_no, product_id))
                t.start()
            else:
                return{"message": status},200
            return{"message" :"Starting to Create Database"},201
        elif cmd == "add":
            if not status:
                status = "Adding New Data"
                t = Thread(target=do_add_command, args=(payment_no, product_id))
                t.start()
            else:
                return{"message": status},200
                
            return{"message":"Starting to Add New Data"},201
        else:
            abort(404,message="Command Failed :/command=create สร้างใหม่ :/command=add เพิ่มข้อมูล ")
api.add_resource(RecommendationSystem,
    '/get_rule/items_base=<string:product_id>',
    '/get_rule/items_base=<string:product_id>/lift><float(signed=True):lift>',
    '/get_rule/items_base=<string:product_id>/confidence><float(signed=True):confidence>',
    '/get_rule/items_base=<string:product_id>/lift><float(signed=True):lift>/confidence><float(signed=True):confidence>'
    )
api.add_resource(DatabaseManager,
    '/', '/status','/status/command=<string:cmd>',
    '/post',
    '/post/command=<string:cmd>',
    '/post/command=<string:cmd>/column-payment-no=<string:payment_no>/column-product-id=<string:product_id>'
    )

def do_create_command(payment_no,product):
    global status
    try:
        transaform_data = transform_to_transactions(payment_no,product)
        clean_payment_one_item = [x for x in transaform_data if len(x) != 1]
        recive_rule = apriori(clean_payment_one_item)
        rule_records = [x for x in manage_rule(recive_rule)]
        save_rule = []
        for rule_record in rule_records:
            rule = Rule(
                items_base = rule_record[0],
                items_add = rule_record[1],
                transaction_itemsets = rule_record[2],
                transaction_itemset = rule_record[3],
                transaction_items_base = rule_record[4],
                transaction_items_add = rule_record[5],
                lift = rule_record[6],
                confidence = rule_record[7]      
            )
            save_rule.append(rule)
        with app.app_context():
            db.session.add_all(save_rule)
            db.session.commit()
        status = 0
    except:
        status = "API create error DatabaseManager ERROR"

def do_add_command(payment_no,product):
    global status
    try:
        transaform_data = transform_to_transactions(payment_no,product)
        clean_payment_one_item = [x for x in transaform_data if len(x) != 1]
        recive_rule = apriori(clean_payment_one_item)
        rule_records = [x for x in manage_rule(recive_rule)]
        with app.app_context():
            load_transaction = Rule.query.with_entities(Rule.transaction_itemsets).first()
        if not load_transaction :
            load_transaction = 0  # first run
        else:
            load_transaction = load_transaction[0]
        save_rule = []
        for rule_record in rule_records:
            with app.app_context():
                obsolete = Rule.query.filter((Rule.items_base == rule_record[0]) & (Rule.items_add == rule_record[1])).first()
                if not obsolete: #insert new rule            
                    transaction_itemsets_new = load_transaction + rule_record[2]
                    confidence_new = ((rule_record[3]/transaction_itemsets_new)
                        / (rule_record[4]/transaction_itemsets_new)
                        )
                    lift_new = confidence_new / (rule_record[5]/transaction_itemsets_new)
                    rule = Rule(
                        items_base = rule_record[0],
                        items_add = rule_record[1],
                        transaction_itemsets = transaction_itemsets_new,
                        transaction_itemset = rule_record[3],
                        transaction_items_base = rule_record[4],
                        transaction_items_add = rule_record[5],
                        lift = lift_new,
                        confidence = confidence_new      
                    )
                    save_rule.append(rule)
                    continue
                # update_rule
                obsolete.transaction_itemsets += rule_record[2]
                obsolete.transaction_itemset += rule_record[3]
                obsolete.transaction_items_base += rule_record[4]
                obsolete.transaction_items_add += rule_record[5]
                obsolete.confidence = (
                    (obsolete.transaction_itemset/obsolete.transaction_itemsets)
                    / (obsolete.transaction_items_base/obsolete.transaction_itemsets)
                    )
                obsolete.lift = obsolete.confidence / (
                    obsolete.transaction_items_add/obsolete.transaction_itemsets
                    )
                db.session.commit()
        if save_rule:
            with app.app_context():
                db.session.add_all(save_rule)
                db.session.commit()
        with app.app_context():
            obsoletes = Rule.query.filter_by(transaction_itemsets=load_transaction).all()
            for  obsolete in obsoletes:
                obsolete.transaction_itemsets += rule_records[0][2]
                obsolete.confidence = (
                    (obsolete.transaction_itemset/obsolete.transaction_itemsets)
                    / (obsolete.transaction_items_base/obsolete.transaction_itemsets)
                    )
                obsolete.lift = obsolete.confidence / (
                    obsolete.transaction_items_add/obsolete.transaction_itemsets
                    )
                db.session.commit()
        status = 0
    except:
        status = "API add error DatabaseManager ERROR"
if __name__ == "__main__":  
    app.run()
